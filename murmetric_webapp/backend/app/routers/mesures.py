"""Requêtage InfluxDB générique pour les 3 types de mesures du projet
(HR/T capteurs BLE, retrait DeweSoft, teneur en eau) — alimente à la fois
l'abaque (vue d'ensemble) et l'agrégation utilisée par l'assistant IA."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from .. import config
from ..influx import MESURE_CAPTEURS, MESURE_DEWESOFT, MESURE_TENEUR_EAU, flux_escape, query_api

router = APIRouter(prefix="/api/mesures", tags=["mesures"])

TypeMesure = Literal["hr_t", "retrait", "teneur_eau"]

_MESURE_PAR_TYPE = {
    "hr_t": MESURE_CAPTEURS,
    "retrait": MESURE_DEWESOFT,
    "teneur_eau": MESURE_TENEUR_EAU,
}
_CHAMPS_PAR_TYPE = {
    "hr_t": ["temperature", "humidite", "point_de_rosee"],
    "retrait": ["valeur", "valeur_filtree"],
    "teneur_eau": ["teneur_eau_pourcent"],
}


# Fenêtre par défaut quand ni `debut` ni `fin` ne sont fournis — 30 jours
# pour "retrait" seulement : mesures_dewesoft est ~1,5 milliard de points
# (échantillonnage 100 Hz, cf. section 32), une agrégation sur 365 jours y
# dépasse régulièrement 30s même avec un seul canal filtré (testé en
# conditions réelles le 12/08/2026, sur ce VPS partagé sans GPU). hr_t/
# teneur_eau restent sur 365 jours : volumes négligeables (dizaines de
# milliers de points), la requête est instantanée quelle que soit la plage.
_FENETRE_DEFAUT_JOURS = {"hr_t": 365, "retrait": 30, "teneur_eau": 365}


def _valider_bornes(debut: str | None, fin: str | None, type_mesure: TypeMesure = "hr_t") -> tuple[str, str]:
    fin_dt = datetime.fromisoformat(fin.replace("Z", "+00:00")) if fin else datetime.now(timezone.utc)
    jours = _FENETRE_DEFAUT_JOURS[type_mesure]
    debut_dt = datetime.fromisoformat(debut.replace("Z", "+00:00")) if debut else fin_dt - timedelta(days=jours)
    return debut_dt.isoformat(), fin_dt.isoformat()


def construire_requete_flux(
    type_mesure: TypeMesure,
    mur: str | None,
    couche: str | None,
    position: str | None,
    canal_nom: str | None,
    debut: str,
    fin: str,
    fenetre: str | None,
) -> str:
    mesure = _MESURE_PAR_TYPE[type_mesure]
    champs = _CHAMPS_PAR_TYPE[type_mesure]

    filtres = [f'r._measurement == "{mesure}"']
    filtres.append("(" + " or ".join(f'r._field == "{c}"' for c in champs) + ")")
    if mur:
        filtres.append(f'r.nom_mur == "{flux_escape(mur)}"' if type_mesure != "teneur_eau" else f'r.mur == "{flux_escape(mur)}"')
    if couche:
        filtres.append(f'r.nom_couche == "{flux_escape(couche)}"' if type_mesure != "teneur_eau" else f'r.couche == "{flux_escape(couche)}"')
    if position and type_mesure in ("hr_t", "retrait"):
        filtres.append(f'r.position == "{flux_escape(position)}"')
    if canal_nom and type_mesure == "retrait":
        filtres.append(f'r.canal_nom == "{flux_escape(canal_nom)}"')

    clause_filtre = "\n  |> filter(fn: (r) => " + ")\n  |> filter(fn: (r) => ".join(filtres) + ")"
    agregation = f'\n  |> aggregateWindow(every: {fenetre}, fn: mean, createEmpty: false)' if fenetre else ""

    return (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: {debut}, stop: {fin})"
        f"{clause_filtre}"
        f"{agregation}\n"
        f'  |> sort(columns: ["_time"])'
    )


def executer_requete(flux: str) -> list[dict]:
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:  # connexion InfluxDB indisponible, requête invalide...
        raise HTTPException(status_code=502, detail=f"Requête InfluxDB échouée : {exc}") from exc

    resultats = []
    for table in tables:
        for record in table.records:
            valeurs = record.values
            point = {
                "time": record.get_time().isoformat(),
                "field": record.get_field(),
                "value": record.get_value(),
            }
            for tag in ("nom_mur", "mur", "nom_couche", "couche", "position", "canal_nom", "utilisateur_nom", "commentaire"):
                if tag in valeurs:
                    point[tag] = valeurs[tag]
            resultats.append(point)
    return resultats


def calculer_statistiques(
    type_mesure: TypeMesure,
    mur: str | None,
    couche: str | None,
    position: str | None,
    canal_nom: str | None,
    debut: str,
    fin: str,
) -> dict:
    """Stats pré-agrégées (min/max/mean/count) — jamais de points bruts
    envoyés à l'assistant IA, cf. section 32 (garde-fou coût/fiabilité)."""
    champ_principal = _CHAMPS_PAR_TYPE[type_mesure][0]
    mesure = _MESURE_PAR_TYPE[type_mesure]

    filtres = [f'r._measurement == "{mesure}"', f'r._field == "{champ_principal}"']
    if mur:
        filtres.append(f'r.nom_mur == "{flux_escape(mur)}"' if type_mesure != "teneur_eau" else f'r.mur == "{flux_escape(mur)}"')
    if couche:
        filtres.append(f'r.nom_couche == "{flux_escape(couche)}"' if type_mesure != "teneur_eau" else f'r.couche == "{flux_escape(couche)}"')
    if position and type_mesure in ("hr_t", "retrait"):
        filtres.append(f'r.position == "{flux_escape(position)}"')
    if canal_nom and type_mesure == "retrait":
        filtres.append(f'r.canal_nom == "{flux_escape(canal_nom)}"')
    clause_filtre = "\n  |> filter(fn: (r) => " + ")\n  |> filter(fn: (r) => ".join(filtres) + ")"

    stats: dict = {"champ": champ_principal, "mur": mur, "couche": couche, "debut": debut, "fin": fin}

    # Les 4 agrégats natifs (min/max/mean/count, optimisés par InfluxDB —
    # nettement plus rapides qu'un reduce() générique passé par la VM Flux
    # point par point, testé en conditions réelles le 12/08/2026 sur
    # mesures_dewesoft/retrait, ~1,5 milliard de points : le reduce() dépassait
    # encore le timeout là où min()/max()/mean()/count() natifs passent) sont
    # lancés en parallèle plutôt qu'en séquence — le temps total tombe alors
    # au niveau du plus lent des 4, pas de leur somme.
    def _executer(nom_fonction: str) -> tuple | None:
        flux = (
            f'from(bucket: "{config.INFLUX_BUCKET}")\n'
            f"  |> range(start: {debut}, stop: {fin})"
            f"{clause_filtre}\n"
            f"  |> {nom_fonction}()"
        )
        tables = query_api().query(flux, org=config.INFLUX_ORG)
        for table in tables:
            for record in table.records:
                return record.get_value()
        return None

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futurs = {nom: executor.submit(_executer, fn) for nom, fn in (("minimum", "min"), ("maximum", "max"), ("moyenne", "mean"), ("nombre_points", "count"))}
            for nom, futur in futurs.items():
                stats[nom] = futur.result()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Requête statistique échouée : {exc}") from exc
    return stats


_TAGS_PAR_TYPE = {
    "hr_t": ["nom_mur", "nom_couche", "position"],
    "retrait": ["nom_mur", "nom_couche", "position", "canal_nom"],
    "teneur_eau": ["mur", "couche"],
}


def _valeurs_tags_retrait() -> list[dict]:
    """Cas particulier : mesures_dewesoft est trop dense (~1,5 milliard de
    points, cf. section 32) pour qu'un group()+count() sur 2 ans reste
    rapide — testé en direct le 12/08/2026, timeout à 30s. capteurs_retrait.json
    est déjà la source de vérité pour le mapping canal→mur/couche/position
    (c'est lui qui pilote le tagging à l'ingestion), donc pas besoin
    d'interroger InfluxDB pour la même information."""
    from .capteurs import _lire_json  # import différé : évite un cycle au chargement du module

    registre = _lire_json(config.CAPTEURS_RETRAIT_JSON)
    return [
        {
            "nom_mur": infos.get("nom_mur"),
            "nom_couche": infos.get("nom_couche"),
            "position": infos.get("position"),
            "canal_nom": canal,
        }
        for canal, infos in registre.items()
        if canal != "_schema"
    ]


@router.get("/valeurs-tags")
def valeurs_tags(type: TypeMesure = Query(...)) -> dict:
    """Combinaisons mur/couche/position réellement présentes en base — les
    valeurs sont des chaînes libres (ex. "interface carreau et exterieur",
    pas "carreau_ext", cf. logique_projet.md section 32) et pas toujours
    cohérentes en casse ("Milieu carreau" vs "milieu carreau") : plutôt que
    deviner un nom canonique côté frontend, on liste ce qui existe vraiment
    pour peupler des menus déroulants plutôt que des champs texte libres.

    Fenêtre large (2 ans) et pas "30 derniers jours" : les données HR/T et
    teneur en eau actuelles sont un backfill historique (respectivement
    jusqu'à mai et mars 2026, cf. logique_projet.md sections 29/16) — le Pi
    de collecte live n'est pas encore déployé à Amiens — donc une fenêtre
    récente serait vide, pas juste plus rapide."""
    if type == "retrait":
        return {"type": type, "combinaisons": _valeurs_tags_retrait()}

    mesure = _MESURE_PAR_TYPE[type]
    champ_principal = _CHAMPS_PAR_TYPE[type][0]
    tags = _TAGS_PAR_TYPE[type]
    colonnes_flux = "[" + ", ".join(f'"{t}"' for t in tags) + "]"  # Flux veut des guillemets doubles, pas le repr() Python

    flux = (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: -2y)\n"
        f'  |> filter(fn: (r) => r._measurement == "{mesure}")\n'
        f'  |> filter(fn: (r) => r._field == "{champ_principal}")\n'
        f"  |> group(columns: {colonnes_flux})\n"
        f"  |> count()\n"
        f"  |> group()"
    )
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Requête InfluxDB échouée : {exc}") from exc

    combinaisons = []
    for table in tables:
        for record in table.records:
            entree = {tag: record.values.get(tag) for tag in tags}
            entree["nombre_points"] = record.get_value()
            combinaisons.append(entree)
    return {"type": type, "combinaisons": combinaisons}


@router.get("/statistiques")
def statistiques(
    type: TypeMesure = Query(...),
    mur: str | None = None,
    couche: str | None = None,
    position: str | None = None,
    canal_nom: str | None = None,
    debut: str | None = None,
    fin: str | None = None,
):
    debut_iso, fin_iso = _valider_bornes(debut, fin, type)
    return calculer_statistiques(type, mur, couche, position, canal_nom, debut_iso, fin_iso)


@router.get("")
def lister_mesures(
    type: TypeMesure = Query(..., description="hr_t | retrait | teneur_eau"),
    mur: str | None = None,
    couche: str | None = None,
    position: str | None = None,
    canal_nom: str | None = None,
    debut: str | None = Query(None, description="ISO 8601, défaut : 1 an avant fin (30 jours si type=retrait)"),
    fin: str | None = Query(None, description="ISO 8601, défaut : maintenant"),
    fenetre: str | None = Query(None, description="Fenêtre d'agrégation Flux, ex. '1h', '1d' — absent = points bruts"),
):
    debut_iso, fin_iso = _valider_bornes(debut, fin, type)
    flux = construire_requete_flux(type, mur, couche, position, canal_nom, debut_iso, fin_iso, fenetre)
    return {"requete_flux": flux, "points": executer_requete(flux)}


@router.get("/croisement")
def croisement(
    type: TypeMesure = Query(..., description="hr_t | retrait — champs de la même mesure uniquement (ex. temperature/humidite/point_de_rosee)"),
    mur: str | None = None,
    couche: str | None = None,
    position: str | None = None,
    champ_x: str = Query(...),
    champ_y: str = Query(...),
    champ_z: str | None = Query(None, description="Optionnel — 3e grandeur pour un nomogramme 3D (ex. hr_t : temperature/humidite/point_de_rosee)"),
    debut: str | None = None,
    fin: str | None = None,
    fenetre: str | None = Query("10m", description="Fenêtre d'agrégation avant croisement — évite des milliers de points bruts non alignés dans le temps"),
) -> dict:
    """Points appariés (x, y[, z]) au même horodatage — nomogramme
    (section 32 : portage scopé au croisement de champs d'une même mesure,
    ex. température/humidité/point de rosée ; le croisement avec la
    teneur en eau, mesure distincte et éparse nécessitant une jointure "au
    plus proche dans le temps" cf. section 16, n'est pas dans ce périmètre)."""
    champs_demandes = [champ_x, champ_y] + ([champ_z] if champ_z else [])
    if type == "teneur_eau" or any(c not in _CHAMPS_PAR_TYPE[type] for c in champs_demandes):
        raise HTTPException(status_code=400, detail="champ_x/champ_y/champ_z doivent appartenir à la même mesure (hr_t ou retrait).")

    debut_iso, fin_iso = _valider_bornes(debut, fin, type)
    mesure = _MESURE_PAR_TYPE[type]
    clause_champs = " or ".join(f'r._field == "{c}"' for c in champs_demandes)
    filtres = [f'r._measurement == "{mesure}"', f"({clause_champs})"]
    if mur:
        filtres.append(f'r.nom_mur == "{flux_escape(mur)}"')
    if couche:
        filtres.append(f'r.nom_couche == "{flux_escape(couche)}"')
    if position:
        filtres.append(f'r.position == "{flux_escape(position)}"')
    clause_filtre = "\n  |> filter(fn: (r) => " + ")\n  |> filter(fn: (r) => ".join(filtres) + ")"
    clause_exists = " and ".join(f"exists r.{c}" for c in champs_demandes)

    flux = (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: {debut_iso}, stop: {fin_iso})"
        f"{clause_filtre}\n"
        f"  |> aggregateWindow(every: {fenetre}, fn: mean, createEmpty: false)\n"
        f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
        f"  |> filter(fn: (r) => {clause_exists})\n"
        f'  |> sort(columns: ["_time"])'
    )
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Requête InfluxDB échouée : {exc}") from exc

    points = []
    for table in tables:
        for record in table.records:
            valeurs = record.values
            point = {"time": record.get_time().isoformat(), "x": valeurs.get(champ_x), "y": valeurs.get(champ_y)}
            if champ_z:
                point["z"] = valeurs.get(champ_z)
            points.append(point)
    return {"champ_x": champ_x, "champ_y": champ_y, "champ_z": champ_z, "points": points}
