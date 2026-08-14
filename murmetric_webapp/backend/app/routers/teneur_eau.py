"""Saisie manuelle de la teneur en eau — conception figée en section 16 de
logique_projet.md : écriture directe InfluxDB (pas de Kafka, saisie humaine
ponctuelle), _time = date de mesure terrain, correction d'un tag/timestamp
= delete-by-predicate + réécriture (pas une UPDATE SQL)."""

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from .. import config
from ..auth import utilisateur_courant
from ..influx import (
    MESURE_TENEUR_EAU,
    delete_points,
    echap_field_str,
    echap_tag,
    flux_escape,
    write_point,
)
from .mesures import executer_requete

router = APIRouter(prefix="/api/teneur_eau", tags=["teneur_eau"])


class SaisieTeneurEau(BaseModel):
    """Nouvelle mesure de teneur en eau saisie manuellement."""

    mur: str
    couche: str
    valeur_pourcent: float = Field(
        ...,
        description=(
            "Arrondi serveur à 2 décimales — cf. correction précision du " "backfill historique"
        ),
    )
    commentaire: str = ""
    date_mesure: datetime | None = Field(None, description="Défaut : maintenant")


class CorrectionTeneurEau(BaseModel):
    """Correction d'une mesure existante, ciblée par son triplet (mur, couche, date) original."""

    mur_original: str
    couche_original: str
    date_mesure_original: datetime
    mur: str
    couche: str
    valeur_pourcent: float
    commentaire: str = ""
    date_mesure: datetime


def _construire_ligne(
    utilisateur: dict, mur: str, couche: str, valeur: float, commentaire: str, date_mesure: datetime
) -> str:
    tags = (
        f"utilisateur_id={echap_tag(utilisateur['username'])},"
        f"utilisateur_nom={echap_tag(utilisateur['nom_affiche'])},"
        f"mur={echap_tag(mur)},"
        f"couche={echap_tag(couche)},"
        f"prestation={echap_tag('Non défini')}"
    )
    fields = f'teneur_eau_pourcent={round(valeur, 2)},commentaire="{echap_field_str(commentaire)}"'
    ts_ns = int(date_mesure.timestamp() * 1_000_000_000)
    return f"{MESURE_TENEUR_EAU},{tags} {fields} {ts_ns}"


@router.get("")
def lister() -> list[dict]:
    """Liste toutes les mesures de teneur en eau, les plus récentes en premier."""
    flux = (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: 0)\n"
        f'  |> filter(fn: (r) => r._measurement == "{MESURE_TENEUR_EAU}")\n'
        f'  |> filter(fn: (r) => r._field == "teneur_eau_pourcent" or r._field == "commentaire")\n'
        f'  |> sort(columns: ["_time"], desc: true)'
    )
    return executer_requete(flux)


@router.post("")
def creer(saisie: SaisieTeneurEau, utilisateur: dict = Depends(utilisateur_courant)) -> dict:
    """Enregistre une nouvelle mesure de teneur en eau."""
    date_mesure = saisie.date_mesure or datetime.now(timezone.utc)
    ligne = _construire_ligne(
        utilisateur,
        saisie.mur,
        saisie.couche,
        saisie.valeur_pourcent,
        saisie.commentaire,
        date_mesure,
    )
    try:
        write_point(ligne)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Écriture InfluxDB échouée : {exc}") from exc
    return {"statut": "ok", "date_mesure": date_mesure.isoformat()}


@router.put("")
def corriger(
    correction: CorrectionTeneurEau, utilisateur: dict = Depends(utilisateur_courant)
) -> dict:
    """Cible le point original par le triplet exact (mur, couche, date_mesure)
    fourni par le frontend, cf. section 16 — pas d'ID auto-incrémenté en InfluxDB."""
    identite_inchangee = (
        correction.mur == correction.mur_original
        and correction.couche == correction.couche_original
        and correction.date_mesure == correction.date_mesure_original
    )
    if not identite_inchangee:
        predicat = (
            f'_measurement="{MESURE_TENEUR_EAU}" AND '
            f'mur="{flux_escape(correction.mur_original)}" AND '
            f'couche="{flux_escape(correction.couche_original)}"'
        )
        marge = timedelta(seconds=1)
        try:
            delete_points(
                predicat,
                correction.date_mesure_original - marge,
                correction.date_mesure_original + marge,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=502, detail=f"Suppression du point original échouée : {exc}"
            ) from exc

    ligne = _construire_ligne(
        utilisateur,
        correction.mur,
        correction.couche,
        correction.valeur_pourcent,
        correction.commentaire,
        correction.date_mesure,
    )
    try:
        write_point(ligne)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Écriture InfluxDB échouée : {exc}") from exc
    return {"statut": "ok", "identite_changee": not identite_inchangee}
