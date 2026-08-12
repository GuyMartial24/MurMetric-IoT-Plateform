"""Registre capteurs — lecture seule en V1 (section 32 : l'écriture depuis
l'appli est repoussée, capteurs.json/capteurs_retrait.json ne sont pas encore
une source de configuration concurrente-safe)."""
import json

from fastapi import APIRouter, HTTPException

from .. import config

router = APIRouter(prefix="/api/capteurs", tags=["capteurs"])


def _lire_json(chemin) -> dict:
    if not chemin.exists():
        raise HTTPException(status_code=500, detail=f"Fichier introuvable : {chemin}")
    # utf-8-sig : capteurs.json a porté un BOM par le passé (cf. section 30),
    # tolérant même si le fichier actuel n'en a plus.
    with open(chemin, encoding="utf-8-sig") as f:
        return json.load(f)


@router.get("/hr_t")
def capteurs_hr_t() -> dict:
    return _lire_json(config.CAPTEURS_JSON)


@router.get("/retrait")
def capteurs_retrait() -> dict:
    return _lire_json(config.CAPTEURS_RETRAIT_JSON)
