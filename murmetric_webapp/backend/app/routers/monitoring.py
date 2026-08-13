"""Monitoring des pipelines d'ingestion (section 32, 13/08/2026) — deux
signaux complémentaires par pipeline (retrait, hr_t) :

- Fraîcheur des données : dernier point réellement écrit dans InfluxDB pour
  les capteurs actuellement marqués `ingestion: true` (pas tout le
  registre — un canal/capteur jamais activé n'a jamais de données, ce
  n'est pas une panne). C'est le signal le plus direct : "est-ce que ça
  coule", indépendant de tout ce qui tourne côté PC Amiens/Pi.
- Battement de vie (heartbeat) : dernier message reçu sur
  MQTT_TOPIC_HEARTBEAT, écrit par monitoring_mqtt.py. Renseigne sur l'état
  du process lui-même (connecté au broker ? buffer SQLite en attente ?
  registre capteurs récupéré depuis l'API ?) — complète la fraîcheur en
  expliquant le "pourquoi" quand elle se dégrade.

Un pipeline sans aucune source active (aucun capteur/canal à
`ingestion: true`) est signalé "inactif", pas "critique" : ce n'est pas une
panne, juste rien à surveiller pour l'instant.
"""
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from .. import config
from ..influx import MESURE_CAPTEURS, MESURE_DEWESOFT, MESURE_HEARTBEAT, flux_escape, query_api

router = APIRouter(prefix="/api/monitoring", tags=["monitoring"])

# Au-delà de "attention", des messages continuent d'arriver mais avec un
# retard anormal ; au-delà de "critique", plus rien n'arrive. Retrait :
# fichiers DeweSoft rotés ~12h, un cycle complet peut légitimement séparer
# deux écritures. HR/T : capteurs configurables jusqu'à 24h d'intervalle de
# log (lint_max_confirme_s), seuils plus larges en conséquence.
SEUILS_HEURES = {
    "retrait": {"attention": 18, "critique": 36},
    "hr_t": {"attention": 30, "critique": 72},
}


def _canaux_retrait_actifs() -> list[str]:
    from .capteurs import _lire_json  # import différé : évite un cycle au chargement du module

    registre = _lire_json(config.CAPTEURS_RETRAIT_JSON)
    return [c for c, infos in registre.items() if c != "_schema" and infos.get("ingestion")]


def _macs_hrt_actifs() -> list[str]:
    from .capteurs import _lire_json

    registre = _lire_json(config.CAPTEURS_JSON)
    return [m for m, infos in registre.items() if m != "_schema" and infos.get("ingestion")]


def _dernier_point(mesure: str, champ: str, tag: str, valeurs: list[str]) -> datetime | None:
    filtre_valeurs = " or ".join(f'r.{tag} == "{flux_escape(v)}"' for v in valeurs)
    flux = f'''
from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: -90d)
  |> filter(fn: (r) => r._measurement == "{mesure}")
  |> filter(fn: (r) => r._field == "{champ}")
  |> filter(fn: (r) => {filtre_valeurs})
  |> last()
'''
    dernier = None
    for table in query_api().query(flux, org=config.INFLUX_ORG):
        for record in table.records:
            t = record.get_time()
            if t and (dernier is None or t > dernier):
                dernier = t
    return dernier


def _statut(dernier: datetime | None, seuils: dict) -> tuple[str, float | None]:
    if dernier is None:
        return "critique", None
    age_h = (datetime.now(timezone.utc) - dernier).total_seconds() / 3600
    if age_h < seuils["attention"]:
        statut = "ok"
    elif age_h < seuils["critique"]:
        statut = "attention"
    else:
        statut = "critique"
    return statut, age_h


def _dernier_heartbeat_par_pipeline() -> dict[str, dict]:
    """Best-effort : une erreur ici ne doit jamais faire échouer /etat, le
    heartbeat n'est qu'un enrichissement diagnostique, pas la donnée
    principale."""
    flux = f'''
from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: -6h)
  |> filter(fn: (r) => r._measurement == "{MESURE_HEARTBEAT}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["pipeline"])
  |> last(column: "_time")
'''
    resultat: dict[str, dict] = {}
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception:
        return resultat
    for table in tables:
        for record in table.records:
            v = record.values
            pipeline = v.get("pipeline")
            if not pipeline:
                continue
            t = record.get_time()
            resultat[pipeline] = {
                "recu_le": t.isoformat() if t else None,
                "machine": v.get("machine"),
                "mqtt_connecte": bool(v.get("mqtt_connecte")),
                "buffer_sqlite_en_attente": v.get("buffer_sqlite_en_attente"),
                "registre_api_ok": bool(v.get("registre_api_ok")),
                "nb_capteurs_connus": v.get("nb_capteurs_connus"),
                "demarre_le": v.get("demarre_le"),
            }
    return resultat


@router.get("/etat")
def etat() -> dict:
    resultat = {}
    heartbeats = _dernier_heartbeat_par_pipeline()

    try:
        canaux_actifs = _canaux_retrait_actifs()
        if canaux_actifs:
            dernier = _dernier_point(MESURE_DEWESOFT, "valeur", "canal_nom", canaux_actifs)
            statut, age_h = _statut(dernier, SEUILS_HEURES["retrait"])
        else:
            dernier, statut, age_h = None, "inactif", None
        resultat["retrait"] = {
            "statut": statut,
            "dernier_point": dernier.isoformat() if dernier else None,
            "age_heures": round(age_h, 1) if age_h is not None else None,
            "nb_sources_actives": len(canaux_actifs),
            "heartbeat": heartbeats.get("retrait"),
        }

        macs_actifs = _macs_hrt_actifs()
        if macs_actifs:
            dernier = _dernier_point(MESURE_CAPTEURS, "temperature", "adresse_mac", macs_actifs)
            statut, age_h = _statut(dernier, SEUILS_HEURES["hr_t"])
        else:
            dernier, statut, age_h = None, "inactif", None
        resultat["hr_t"] = {
            "statut": statut,
            "dernier_point": dernier.isoformat() if dernier else None,
            "age_heures": round(age_h, 1) if age_h is not None else None,
            "nb_sources_actives": len(macs_actifs),
            "heartbeat": heartbeats.get("hr_t"),
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Requête InfluxDB échouée : {exc}") from exc

    return resultat


@router.get("/heartbeats")
def heartbeats(pipeline: str, heures: int = 24) -> list[dict]:
    flux = f'''
from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: -{int(heures)}h)
  |> filter(fn: (r) => r._measurement == "{MESURE_HEARTBEAT}")
  |> filter(fn: (r) => r.pipeline == "{flux_escape(pipeline)}")
  |> filter(fn: (r) => r._field == "buffer_sqlite_en_attente")
  |> sort(columns: ["_time"])
'''
    try:
        points = [
            {"time": record.get_time().isoformat(), "buffer_sqlite_en_attente": record.get_value()}
            for table in query_api().query(flux, org=config.INFLUX_ORG)
            for record in table.records
        ]
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Requête InfluxDB échouée : {exc}") from exc
    return points
