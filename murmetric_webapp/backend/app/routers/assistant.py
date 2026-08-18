"""Assistant IA — explication de courbe + brouillon de rapport d'instrumentation.

Architecture tranchée en section 32 de logique_projet.md, complétée
section 36 (18/08/2026) :
- Gemini (Google AI Studio, API OpenAI-compatible) fournisseur PRIMAIRE
  depuis le 13/08/2026 — texte ET vision (analyse d'image de graphique,
  cf. /chat-image). Repli automatique sur Groq si Gemini échoue, pour le
  texte ET la vision depuis le 18/08/2026 (`qwen/qwen3.6-27b`, seul modèle
  vision disponible sur ce compte Groq — les anciens
  llama-3.2-*-vision-preview étaient déjà décommissionnés au 13/08/2026,
  et aucun autre modèle testé n'accepte de contenu image).
- Jamais de points bruts envoyés au modèle : les tools exposés ne
  renvoient que des statistiques pré-agrégées (calculer_statistiques,
  comparer_periodes, ecart_brut_filtre) — le mode vision voit une IMAGE
  déjà tracée par l'appli, pas les points sous-jacents.
- Ancrage explicite sur la sélection affichée côté frontend (mur/couche/
  période) — fournie ici via `selection`, pas déduite du texte du prompt.
"""

import json
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from openai import APIStatusError, OpenAI
from pydantic import BaseModel

from .. import config
from ..auth import utilisateur_courant
from ..parametres import (
    obtenir_cle_gemini,
    obtenir_cle_groq,
    obtenir_modele_gemini,
    obtenir_modele_groq,
)
from .mesures import (
    TypeMesure,
    _valider_bornes,
    calculer_statistiques,
    comparer_periodes,
    ecart_brut_filtre,
)

router = APIRouter(prefix="/api/assistant", tags=["assistant"])

Mode = Literal["explain", "report"]

GROQ_BASE_URL = "https://api.groq.com/openai/v1"


class Selection(BaseModel):
    """Sélection mur/couche/période sur laquelle porte la question de l'utilisateur."""

    type: TypeMesure
    mur: str | None = None
    couche: str | None = None
    position: str | None = None
    canal_nom: str | None = None
    debut: str | None = None
    fin: str | None = None


class DemandeChat(BaseModel):
    """Question texte adressée à l'assistant sur une sélection donnée."""

    mode: Mode
    prompt: str
    selection: Selection


class DemandeChatImage(BaseModel):
    """Question adressée à l'assistant, accompagnée d'une image de graphique (mode vision)."""

    mode: Mode
    prompt: str
    image_data_uri: str
    selection: Selection | None = None


_SYSTEME = """Tu es un assistant d'aide à l'analyse pour MurMetric, une plateforme
de monitoring de parois biosourcées (murs SOCMA 1/SOCMA 2, capteurs de
température/humidité, retrait mécanique, teneur en eau). Tu aides un
utilisateur technique (ingénieur R&D) à interpréter des mesures et à
rédiger des rapports d'instrumentation.

Règles strictes :
- Tu ne reçois jamais les mesures brutes, seulement des statistiques déjà
  agrégées via tes outils. Utilise-les pour obtenir des chiffres précis
  avant toute affirmation quantitative — ne jamais inventer une valeur.
- interroger_statistiques_mesures renvoie, par grandeur du type
  sélectionné (ex. "hr_t" → temperature/humidite/point_de_rosee
  ensemble) : minimum/maximum/moyenne/mediane/nombre_points, une
  "tendance" (moyenne 1ère vs 2e moitié de la période) et, pour "hr_t"
  uniquement, une "amplitude_jour_nuit". Tu peux et dois commenter
  n'importe quelle grandeur selon la question posée, pas seulement la
  première.
- comparer_deux_periodes : à utiliser si la question porte explicitement
  sur une comparaison temporelle ("par rapport au mois dernier"...).
- ecart_brut_filtre_retrait : à utiliser si la question porte sur le
  bruit/la qualité du filtrage du retrait (jamais de recalcul Hampel sur
  une longue période, plafonné à 2h par ailleurs).
- Si le mode est "report" : rédige un brouillon structuré de rapport
  d'instrumentation (contexte, mesures, observations, limites), en
  rappelant explicitement qu'il s'agit d'un brouillon à relire et valider
  par un humain avant tout usage réel. Ce rappel forme TOUJOURS son
  propre paragraphe final, commençant littéralement par "Note : " (ex.
  "Note : ce brouillon doit être relu et validé par un humain avant tout
  usage réel."), jamais mélangé au reste du texte.
- Si le mode est "explain" : explique la sélection affichée (tendance,
  amplitude, points remarquables) en langage clair, sans jargon inutile.
- N'utilise JAMAIS de syntaxe Markdown : pas de **gras**, pas de titres
  avec #, pas de listes à puces avec -/*, pas de lignes horizontales ---,
  pas de notation mathématique avec $...$. Écris en prose naturelle,
  comme dans une note technique ou un email, pas comme un rapport généré
  par IA affichant sa mise en forme brute. Pour "report", structure avec
  des paragraphes et des intitulés de section en texte simple suivis de
  deux-points (ex. "Contexte :", "Observations :"), jamais de titres
  Markdown."""

_SYSTEME_VISION = """Tu es un assistant d'aide à l'analyse pour MurMetric, une plateforme
de monitoring de parois biosourcées. Tu reçois ici l'IMAGE d'un graphique
déjà tracé par l'application (jamais les points bruts sous-jacents),
accompagnée si disponible de statistiques pré-agrégées de la même
sélection. Décris ce que tu observes visuellement (tendance, cycles,
ruptures, points remarquables, comparaison entre courbes) et recoupe avec
les chiffres fournis avant toute affirmation quantitative précise — ne
jamais inventer une valeur absente des statistiques fournies. Rappelle
qu'il s'agit d'une lecture assistée par IA, à vérifier visuellement par
un humain avant tout usage réel — ce rappel forme TOUJOURS son propre
paragraphe final, commençant littéralement par "Note : " (ex. "Note :
il s'agit d'une lecture assistée par intelligence artificielle, à
vérifier visuellement par un humain avant tout usage réel."), jamais
mélangé au reste du texte. N'utilise JAMAIS de syntaxe Markdown
(pas de **gras**, pas de titres avec #, pas de listes à puces, pas de
lignes horizontales ---, pas de notation $...$) : écris en prose
naturelle, comme dans une note technique, pas comme un rapport généré
par IA affichant sa mise en forme brute. Écris un français rigoureusement
correct et complet, avec TOUS les accents (é, è, ê, à, ù...) et TOUTES
les apostrophes (c'est, l'analyse, n'est pas, d'un...) — jamais de forme
simplifiée ou translittérée sans diacritiques."""

_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "interroger_statistiques_mesures",
            "description": (
                "Renvoie des statistiques pré-agrégées (min, max, moyenne, médiane, tendance, "
                "nombre de points) pour toutes les grandeurs du type demandé (ex. 'hr_t' renvoie "
                "temperature, humidite ET point_de_rosee) sur une sélection de mesures MurMetric. "
                "Ne renvoie jamais de points bruts."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["hr_t", "retrait", "teneur_eau"]},
                    "mur": {"type": "string", "description": "ex. 'SOCMA 1'"},
                    "couche": {"type": "string", "description": "ex. 'milieu isolant'"},
                    "position": {"type": "string"},
                    "canal_nom": {"type": "string", "description": "retrait uniquement, ex. 'HA1'"},
                    "debut": {"type": "string", "description": "ISO 8601"},
                    "fin": {"type": "string", "description": "ISO 8601"},
                },
                "required": ["type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "comparer_deux_periodes",
            "description": (
                "Compare les statistiques (dont la moyenne) entre deux périodes explicites pour "
                "la même sélection — pour une question du type 'comment ça se compare au mois "
                "dernier'. Ne renvoie jamais de points bruts."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["hr_t", "retrait", "teneur_eau"]},
                    "mur": {"type": "string"},
                    "couche": {"type": "string"},
                    "position": {"type": "string"},
                    "canal_nom": {"type": "string", "description": "retrait uniquement"},
                    "debut_1": {"type": "string", "description": "ISO 8601, début période 1"},
                    "fin_1": {"type": "string", "description": "ISO 8601, fin période 1"},
                    "debut_2": {"type": "string", "description": "ISO 8601, début période 2"},
                    "fin_2": {"type": "string", "description": "ISO 8601, fin période 2"},
                },
                "required": ["type", "debut_1", "fin_1", "debut_2", "fin_2"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ecart_brut_filtre_retrait",
            "description": (
                "Écart moyen absolu entre retrait brut et filtré pour un canal — indicateur de "
                "l'ampleur du bruit/filtrage sur la période, sans recalculer Hampel (coûteux sur "
                "une longue période). Retrait uniquement."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "canal_nom": {"type": "string", "description": "ex. 'HA1'"},
                    "debut": {"type": "string", "description": "ISO 8601"},
                    "fin": {"type": "string", "description": "ISO 8601"},
                },
                "required": ["canal_nom"],
            },
        },
    },
]


def _executer_tool(nom: str, entree: dict) -> dict:
    if nom == "interroger_statistiques_mesures":
        debut_iso, fin_iso = _valider_bornes(entree.get("debut"), entree.get("fin"), entree["type"])
        return calculer_statistiques(
            entree["type"],
            entree.get("mur"),
            entree.get("couche"),
            entree.get("position"),
            entree.get("canal_nom"),
            debut_iso,
            fin_iso,
        )
    if nom == "comparer_deux_periodes":
        debut1_iso, fin1_iso = _valider_bornes(
            entree.get("debut_1"), entree.get("fin_1"), entree["type"]
        )
        debut2_iso, fin2_iso = _valider_bornes(
            entree.get("debut_2"), entree.get("fin_2"), entree["type"]
        )
        return comparer_periodes(
            entree["type"],
            entree.get("mur"),
            entree.get("couche"),
            entree.get("position"),
            entree.get("canal_nom"),
            debut1_iso,
            fin1_iso,
            debut2_iso,
            fin2_iso,
        )
    if nom == "ecart_brut_filtre_retrait":
        debut_iso, fin_iso = _valider_bornes(entree.get("debut"), entree.get("fin"), "retrait")
        return ecart_brut_filtre(entree.get("canal_nom"), debut_iso, fin_iso)
    return {"erreur": f"Outil inconnu : {nom}"}


def _quota_journalier_epuise(exc: APIStatusError) -> bool:
    """Détecte un 429 causé par un quota JOURNALIER (pas un débit par
    minute) via `quotaId` dans le corps d'erreur Gemini — repéré en usage
    réel le 18/08/2026 : le champ `retryDelay` renvoyé à côté (quelques
    secondes) reste trompeur pour ce cas précis, le quota ne se
    réinitialise en réalité qu'après plusieurs heures."""
    try:
        corps = exc.response.json()
        erreur = (corps[0] if isinstance(corps, list) else corps).get("error", {})
        for detail in erreur.get("details", []):
            if detail.get("@type", "").endswith("QuotaFailure"):
                for violation in detail.get("violations", []):
                    if "PerDay" in violation.get("quotaId", ""):
                        return True
    except Exception:
        pass
    return False


def _message_erreur_ia(fournisseur: str, exc: Exception) -> str:
    """Traduit une exception du SDK OpenAI (utilisé pour Gemini ET Groq, tous deux via
    une API compatible) en message lisible — sans ça, une erreur 429/401 remonte le
    corps JSON brut de la réponse (dict Python imbriqué) jusqu'à l'utilisateur."""
    if isinstance(exc, APIStatusError):
        if exc.status_code == 429:
            if _quota_journalier_epuise(exc):
                return (
                    f"{fournisseur} : quota JOURNALIER atteint (plan gratuit, limite très basse) — "
                    "ne se réinitialisera pas avant plusieurs heures, pas dans quelques secondes. "
                    "Passe sur un plan payant si l'usage doit être plus soutenu."
                )
            return (
                f"{fournisseur} : quota d'appels atteint — "
                "réessaie plus tard ou augmente le plan associé à la clé API."
            )
        if exc.status_code == 401:
            return f"{fournisseur} : clé API invalide ou expirée."
        return f"{fournisseur} : erreur API (code {exc.status_code})."
    return f"{fournisseur} : {exc}"


def _completer_avec_outils(client: OpenAI, modele: str, messages: list[dict]) -> str:
    """Boucle tool-use bornée (4 allers-retours max) — évite un
    enchaînement d'appels non maîtrisé côté coût/latence.

    `messages` est muté en place. En cas d'échec (exception ou épuisement
    des itérations), l'appelant doit repartir d'une liste FRAÎCHE pour un
    éventuel essai avec un autre fournisseur plutôt que de réutiliser
    celle-ci — les messages assistant peuvent contenir des champs propres
    au fournisseur (ex. les "thought_signature" de Gemini), pas garantis
    compatibles avec un autre.
    """
    for _ in range(4):
        reponse = client.chat.completions.create(
            model=modele, max_tokens=2000, messages=messages, tools=_TOOLS
        )
        choix = reponse.choices[0]
        if choix.finish_reason != "tool_calls" or not choix.message.tool_calls:
            return choix.message.content
        messages.append(choix.message.model_dump(exclude_none=True))
        for appel in choix.message.tool_calls:
            arguments = json.loads(appel.function.arguments)
            resultat = _executer_tool(appel.function.name, arguments)
            messages.append({"role": "tool", "tool_call_id": appel.id, "content": str(resultat)})
    raise RuntimeError("trop d'itérations d'outils sans réponse finale")


def _messages_initiaux(demande: DemandeChat, stats_initiales: dict) -> list[dict]:
    return [
        {"role": "system", "content": _SYSTEME},
        {
            "role": "user",
            "content": (
                f"Sélection actuellement affichée : {stats_initiales}\n\n"
                f"Question de l'utilisateur ({demande.mode}) : {demande.prompt}"
            ),
        },
    ]


@router.post("/chat")
def chat(demande: DemandeChat, _actuel: dict = Depends(utilisateur_courant)) -> dict:
    """Chat texte — Gemini en premier, repli automatique sur Groq si Gemini échoue."""
    debut_iso, fin_iso = _valider_bornes(
        demande.selection.debut, demande.selection.fin, demande.selection.type
    )
    stats_initiales = calculer_statistiques(
        demande.selection.type,
        demande.selection.mur,
        demande.selection.couche,
        demande.selection.position,
        demande.selection.canal_nom,
        debut_iso,
        fin_iso,
    )

    erreurs: list[str] = []

    cle_gemini = obtenir_cle_gemini()
    if cle_gemini:
        try:
            client = OpenAI(api_key=cle_gemini, base_url=config.GEMINI_BASE_URL)
            reponse = _completer_avec_outils(
                client, obtenir_modele_gemini(), _messages_initiaux(demande, stats_initiales)
            )
            return {"reponse": reponse, "fournisseur": "gemini"}
        except Exception as exc:
            erreurs.append(_message_erreur_ia("Gemini", exc))

    cle_groq = obtenir_cle_groq()
    if cle_groq:
        try:
            client = OpenAI(api_key=cle_groq, base_url=GROQ_BASE_URL)
            reponse = _completer_avec_outils(
                client, obtenir_modele_groq(), _messages_initiaux(demande, stats_initiales)
            )
            return {"reponse": reponse, "fournisseur": "groq"}
        except Exception as exc:
            erreurs.append(_message_erreur_ia("Groq", exc))

    if not erreurs:
        raise HTTPException(
            status_code=500, detail="Aucun fournisseur IA configuré (ni Gemini ni Groq)."
        )
    raise HTTPException(
        status_code=502, detail="Tous les fournisseurs IA ont échoué : " + " | ".join(erreurs)
    )


@router.post("/chat-image")
def chat_image(demande: DemandeChatImage, _actuel: dict = Depends(utilisateur_courant)) -> dict:
    """Analyse d'image de graphique — Gemini en premier, repli automatique
    sur Groq (`qwen/qwen3.6-27b`) si Gemini échoue (quota, panne...)."""
    contexte_stats = ""
    if demande.selection is not None:
        try:
            debut_iso, fin_iso = _valider_bornes(
                demande.selection.debut, demande.selection.fin, demande.selection.type
            )
            stats = calculer_statistiques(
                demande.selection.type,
                demande.selection.mur,
                demande.selection.couche,
                demande.selection.position,
                demande.selection.canal_nom,
                debut_iso,
                fin_iso,
            )
            contexte_stats = (
                "Statistiques précises de cette sélection "
                f"(pour ancrer ton interprétation visuelle) : {stats}\n\n"
            )
        except Exception:
            # L'image seule suffit à répondre si les stats échouent — pas bloquant pour ce mode.
            pass

    messages = [
        {"role": "system", "content": _SYSTEME_VISION},
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        f"{contexte_stats}Question de l'utilisateur "
                        f"({demande.mode}) : {demande.prompt}"
                    ),
                },
                {"type": "image_url", "image_url": {"url": demande.image_data_uri}},
            ],
        },
    ]

    erreurs: list[str] = []

    cle_gemini = obtenir_cle_gemini()
    if cle_gemini:
        try:
            client = OpenAI(api_key=cle_gemini, base_url=config.GEMINI_BASE_URL)
            reponse = client.chat.completions.create(
                model=obtenir_modele_gemini(), max_tokens=2000, messages=messages
            )
            return {"reponse": reponse.choices[0].message.content, "fournisseur": "gemini"}
        except Exception as exc:
            erreurs.append(_message_erreur_ia("Gemini", exc))

    cle_groq = obtenir_cle_groq()
    if cle_groq:
        try:
            client = OpenAI(api_key=cle_groq, base_url=GROQ_BASE_URL)
            reponse = client.chat.completions.create(
                model=config.GROQ_VISION_MODEL,
                max_tokens=2000,
                messages=messages,
                # Modèle "raisonneur" par défaut : un premier essai avec
                # `reasoning_format: hidden` seul a consommé tout le budget
                # de tokens en raisonnement caché, renvoyant une réponse
                # vide (constaté en usage réel le 18/08/2026, malgré
                # max_tokens=4000). `reasoning_effort: none` désactive le
                # raisonnement entièrement — bien plus fiable pour ce cas
                # d'usage (décrire une image), vérifié : réponse correcte
                # en 33 tokens au lieu d'un budget de plusieurs milliers.
                extra_body={"reasoning_effort": "none"},
            )
            contenu = reponse.choices[0].message.content
            if not contenu or not contenu.strip():
                raise RuntimeError("réponse vide reçue du modèle — réessaie.")
            return {"reponse": contenu, "fournisseur": "groq"}
        except Exception as exc:
            erreurs.append(_message_erreur_ia("Groq", exc))

    if not erreurs:
        raise HTTPException(
            status_code=500, detail="Aucun fournisseur IA configuré (ni Gemini ni Groq)."
        )
    raise HTTPException(
        status_code=502,
        detail="Analyse d'image échouée sur tous les fournisseurs : " + " | ".join(erreurs),
    )
