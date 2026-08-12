from fastapi import APIRouter, Depends
from pydantic import BaseModel

from ..auth import utilisateur_courant
from ..parametres import definir_parametres_groq, masquer, obtenir_cle_groq, obtenir_expiration_groq, obtenir_modele_groq

router = APIRouter(prefix="/api/parametres", tags=["parametres"])


class ParametresGroq(BaseModel):
    groq_api_key: str | None = None
    groq_model: str | None = None
    groq_api_key_expiration: str | None = None  # ISO 8601 (date), informative


@router.get("")
def lire(_actuel: dict = Depends(utilisateur_courant)) -> dict:
    return {
        "groq_api_key_masque": masquer(obtenir_cle_groq()),
        "groq_model": obtenir_modele_groq(),
        "groq_api_key_expiration": obtenir_expiration_groq(),
    }


@router.put("")
def modifier(parametres: ParametresGroq, _actuel: dict = Depends(utilisateur_courant)) -> dict:
    definir_parametres_groq(parametres.groq_api_key, parametres.groq_model, parametres.groq_api_key_expiration)
    return {"statut": "ok"}
