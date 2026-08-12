"""Assistant IA — explication de courbe + brouillon de rapport d'instrumentation.

Architecture tranchée en section 32 de logique_projet.md :
- Groq (API OpenAI-compatible), pas de LLM local (VPS sans GPU) — identifiants
  fournis par l'utilisateur le 12/08/2026 (app "MurMetric_AI"), remplace
  Anthropic initialement documenté ; la justification "API cloud plutôt que
  local" ne change pas, seul le fournisseur change.
- Jamais de points bruts envoyés au modèle : le tool exposé au modèle ne
  renvoie que des statistiques pré-agrégées (calculer_statistiques).
- Ancrage explicite sur la sélection affichée côté frontend (mur/couche/
  période) — fournie ici via `selection`, pas déduite du texte du prompt.
"""
import json
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from openai import OpenAI
from pydantic import BaseModel

from ..auth import utilisateur_courant
from ..parametres import obtenir_cle_groq, obtenir_modele_groq
from .mesures import TypeMesure, _valider_bornes, calculer_statistiques

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


_SYSTEME = """Tu es un assistant d'aide à l'analyse pour MurMetric, une plateforme
de monitoring de parois biosourcées (murs SOCMA 1/SOCMA 2, capteurs de
température/humidité, retrait mécanique, teneur en eau). Tu aides un
utilisateur technique (ingénieur R&D) à interpréter des mesures et à
rédiger des rapports d'instrumentation.

Règles strictes :
- Tu ne reçois jamais les mesures brutes, seulement des statistiques déjà
  agrégées (min/max/moyenne/nombre de points) via l'outil
  interroger_statistiques_mesures. Utilise-le pour obtenir des chiffres
  précis avant toute affirmation quantitative — ne jamais inventer une
  valeur.
- Si le mode est "report" : rédige un brouillon structuré de rapport
  d'instrumentation (contexte, mesures, observations, limites), en
  rappelant explicitement qu'il s'agit d'un brouillon à relire et valider
  par un humain avant tout usage réel.
- Si le mode est "explain" : explique la sélection affichée (tendance,
  amplitude, points remarquables) en langage clair, sans jargon inutile."""

_TOOLS = [{
    "type": "function",
    "function": {
        "name": "interroger_statistiques_mesures",
        "description": (
            "Renvoie des statistiques pré-agrégées (min, max, moyenne, nombre de "
            "points) pour une sélection de mesures MurMetric. Ne renvoie jamais "
            "de points bruts."
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
}]


def _executer_tool(nom: str, entree: dict) -> dict:
    if nom != "interroger_statistiques_mesures":
        return {"erreur": f"Outil inconnu : {nom}"}
    debut_iso, fin_iso = _valider_bornes(entree.get("debut"), entree.get("fin"), entree["type"])
    return calculer_statistiques(
        entree["type"], entree.get("mur"), entree.get("couche"),
        entree.get("position"), entree.get("canal_nom"), debut_iso, fin_iso,
    )


@router.post("/chat")
def chat(demande: DemandeChat, _actuel: dict = Depends(utilisateur_courant)) -> dict:
    cle_api = obtenir_cle_groq()
    if not cle_api:
        raise HTTPException(status_code=500, detail="Clé API Groq non configurée (réglages ou GROQ_API_KEY).")

    client = OpenAI(api_key=cle_api, base_url=GROQ_BASE_URL)

    debut_iso, fin_iso = _valider_bornes(demande.selection.debut, demande.selection.fin, demande.selection.type)
    stats_initiales = calculer_statistiques(
        demande.selection.type, demande.selection.mur, demande.selection.couche,
        demande.selection.position, demande.selection.canal_nom, debut_iso, fin_iso,
    )

    messages = [
        {"role": "system", "content": _SYSTEME},
        {
            "role": "user",
            "content": (
                f"Sélection actuellement affichée : {stats_initiales}\n\n"
                f"Question de l'utilisateur ({demande.mode}) : {demande.prompt}"
            ),
        },
    ]

    # Boucle tool-use bornée (4 allers-retours max) — évite un enchaînement
    # d'appels non maîtrisé côté coût/latence.
    for _ in range(4):
        try:
            reponse = client.chat.completions.create(
                model=obtenir_modele_groq(),
                max_tokens=2000,
                messages=messages,
                tools=_TOOLS,
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Appel Groq échoué : {exc}") from exc

        choix = reponse.choices[0]
        if choix.finish_reason != "tool_calls" or not choix.message.tool_calls:
            return {"reponse": choix.message.content}

        messages.append(choix.message.model_dump(exclude_none=True))
        for appel in choix.message.tool_calls:
            arguments = json.loads(appel.function.arguments)
            resultat = _executer_tool(appel.function.name, arguments)
            messages.append({"role": "tool", "tool_call_id": appel.id, "content": str(resultat)})

    raise HTTPException(status_code=504, detail="Assistant IA : trop d'itérations d'outils sans réponse finale.")
