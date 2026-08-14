"""Assistant IA — explication de courbe + brouillon de rapport d'instrumentation.

Architecture tranchée en section 32 de logique_projet.md :
- Gemini (Google AI Studio, API OpenAI-compatible) fournisseur PRIMAIRE
  depuis le 13/08/2026 — texte ET vision (analyse d'image de graphique,
  cf. /chat-image). Repli automatique sur Groq pour le texte uniquement
  si Gemini échoue (clé fournie par l'utilisateur le 12/08/2026, app
  "MurMetric_AI") — jamais l'inverse pour la vision : aucun modèle vision
  n'est disponible sur ce compte Groq (vérifié en direct le 13/08/2026,
  les anciens llama-3.2-*-vision-preview sont décommissionnés).
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
from openai import OpenAI
from pydantic import BaseModel

from .. import config
from ..auth import utilisateur_courant
from ..parametres import obtenir_cle_gemini, obtenir_cle_groq, obtenir_modele_gemini, obtenir_modele_groq
from .mesures import TypeMesure, _valider_bornes, calculer_statistiques, comparer_periodes, ecart_brut_filtre

router = APIRouter(prefix="/api/assistant", tags=["assistant"])

Mode = Literal["explain", "report"]

GROQ_BASE_URL = "https://api.groq.com/openai/v1"


class Selection(BaseModel):
    type: TypeMesure
    mur: str | None = None
    couche: str | None = None
    position: str | None = None
    canal_nom: str | None = None
    debut: str | None = None
    fin: str | None = None


class DemandeChat(BaseModel):
    mode: Mode
    prompt: str
    selection: Selection


class DemandeChatImage(BaseModel):
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
  par un humain avant tout usage réel.
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
un humain avant tout usage réel. N'utilise JAMAIS de syntaxe Markdown
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
            entree["type"], entree.get("mur"), entree.get("couche"),
            entree.get("position"), entree.get("canal_nom"), debut_iso, fin_iso,
        )
    if nom == "comparer_deux_periodes":
        debut1_iso, fin1_iso = _valider_bornes(entree.get("debut_1"), entree.get("fin_1"), entree["type"])
        debut2_iso, fin2_iso = _valider_bornes(entree.get("debut_2"), entree.get("fin_2"), entree["type"])
        return comparer_periodes(
            entree["type"], entree.get("mur"), entree.get("couche"), entree.get("position"), entree.get("canal_nom"),
            debut1_iso, fin1_iso, debut2_iso, fin2_iso,
        )
    if nom == "ecart_brut_filtre_retrait":
        debut_iso, fin_iso = _valider_bornes(entree.get("debut"), entree.get("fin"), "retrait")
        return ecart_brut_filtre(entree.get("canal_nom"), debut_iso, fin_iso)
    return {"erreur": f"Outil inconnu : {nom}"}


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
        reponse = client.chat.completions.create(model=modele, max_tokens=2000, messages=messages, tools=_TOOLS)
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
    debut_iso, fin_iso = _valider_bornes(demande.selection.debut, demande.selection.fin, demande.selection.type)
    stats_initiales = calculer_statistiques(
        demande.selection.type, demande.selection.mur, demande.selection.couche,
        demande.selection.position, demande.selection.canal_nom, debut_iso, fin_iso,
    )

    erreurs: list[str] = []

    cle_gemini = obtenir_cle_gemini()
    if cle_gemini:
        try:
            client = OpenAI(api_key=cle_gemini, base_url=config.GEMINI_BASE_URL)
            reponse = _completer_avec_outils(client, obtenir_modele_gemini(), _messages_initiaux(demande, stats_initiales))
            return {"reponse": reponse, "fournisseur": "gemini"}
        except Exception as exc:
            erreurs.append(f"Gemini : {exc}")

    cle_groq = obtenir_cle_groq()
    if cle_groq:
        try:
            client = OpenAI(api_key=cle_groq, base_url=GROQ_BASE_URL)
            reponse = _completer_avec_outils(client, obtenir_modele_groq(), _messages_initiaux(demande, stats_initiales))
            return {"reponse": reponse, "fournisseur": "groq"}
        except Exception as exc:
            erreurs.append(f"Groq : {exc}")

    if not erreurs:
        raise HTTPException(status_code=500, detail="Aucun fournisseur IA configuré (ni Gemini ni Groq).")
    raise HTTPException(status_code=502, detail="Tous les fournisseurs IA ont échoué : " + " | ".join(erreurs))


@router.post("/chat-image")
def chat_image(demande: DemandeChatImage, _actuel: dict = Depends(utilisateur_courant)) -> dict:
    """Analyse d'image de graphique — Gemini exclusivement, aucun repli
    possible (Groq n'a pas de modèle vision disponible sur ce compte)."""
    cle_gemini = obtenir_cle_gemini()
    if not cle_gemini:
        raise HTTPException(
            status_code=500,
            detail="Clé API Gemini non configurée — requise pour l'analyse d'image (Groq n'a pas de modèle vision disponible).",
        )

    contexte_stats = ""
    if demande.selection is not None:
        try:
            debut_iso, fin_iso = _valider_bornes(demande.selection.debut, demande.selection.fin, demande.selection.type)
            stats = calculer_statistiques(
                demande.selection.type, demande.selection.mur, demande.selection.couche,
                demande.selection.position, demande.selection.canal_nom, debut_iso, fin_iso,
            )
            contexte_stats = f"Statistiques précises de cette sélection (pour ancrer ton interprétation visuelle) : {stats}\n\n"
        except Exception:
            pass  # l'image seule suffit à répondre si les stats échouent — pas bloquant pour ce mode

    client = OpenAI(api_key=cle_gemini, base_url=config.GEMINI_BASE_URL)
    messages = [
        {"role": "system", "content": _SYSTEME_VISION},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": f"{contexte_stats}Question de l'utilisateur ({demande.mode}) : {demande.prompt}"},
                {"type": "image_url", "image_url": {"url": demande.image_data_uri}},
            ],
        },
    ]
    try:
        reponse = client.chat.completions.create(model=obtenir_modele_gemini(), max_tokens=2000, messages=messages)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Analyse d'image échouée (Gemini) : {exc}") from exc
    return {"reponse": reponse.choices[0].message.content, "fournisseur": "gemini"}
