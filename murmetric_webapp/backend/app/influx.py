"""Client InfluxDB partagé + helpers de requêtage/écriture pour l'API."""

from datetime import datetime, timezone
from functools import lru_cache

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from . import config

MESURE_CAPTEURS = "mesures_capteurs"
MESURE_DEWESOFT = "mesures_dewesoft"
MESURE_TENEUR_EAU = "mesures_teneur_eau"
MESURE_HEARTBEAT = "pipeline_heartbeat"


@lru_cache
def get_client() -> InfluxDBClient:
    """Retourne le client InfluxDB partagé (mis en cache, une seule instance par process)."""
    # Timeout par défaut du client (10s) trop court pour une agrégation sur
    # mesures_dewesoft (retrait, ~1,5 milliard de points, échantillonnage
    # 100 Hz) — vérifié en conditions réelles le 12/08/2026 (read timeout à
    # 10s sur une requête aggregateWindow(1d) filtrée par canal seul, sur un
    # an). 30s->60s le 28/08/2026 : re-testé après l'ajout de Début/Fin au
    # nomogramme (croisement-libre), qui permet désormais une plage
    # explicite dépassant le plafond implicite de 30 jours utilisé jusque-là
    # pour retrait — un croisement teneur_eau x retrait sur ~80 jours filtré
    # sur un seul canal a mis 7 à 11s selon la fenêtre d'agrégation choisie
    # (charge variable du serveur), déjà proche de l'ancienne marge de 30s.
    return InfluxDBClient(
        url=config.INFLUX_URL,
        token=config.INFLUX_TOKEN,
        org=config.INFLUX_ORG,
        timeout=60_000,
    )


def query_api():
    """Retourne l'API de requêtage Flux du client InfluxDB partagé."""
    return get_client().query_api()


def write_point(line: str) -> None:
    """Écrit une ligne au format Line Protocol, de façon synchrone (erreurs remontées)."""
    write_api = get_client().write_api(write_options=SYNCHRONOUS)
    try:
        write_api.write(
            bucket=config.INFLUX_BUCKET,
            org=config.INFLUX_ORG,
            record=line,
            write_precision=WritePrecision.NS,
        )
    finally:
        write_api.close()


def delete_points(predicate: str, start: datetime, stop: datetime) -> None:
    """Supprime les points InfluxDB correspondant au prédicat sur la fenêtre [start, stop]."""
    get_client().delete_api().delete(
        start, stop, predicate, bucket=config.INFLUX_BUCKET, org=config.INFLUX_ORG
    )


def echap_tag(valeur: str) -> str:
    """Échappe une valeur de tag pour le format Line Protocol (virgule, égal, espace)."""
    return (
        str(valeur)
        .replace("\\", "\\\\")
        .replace(",", "\\,")
        .replace("=", "\\=")
        .replace(" ", "\\ ")
    )


def echap_field_str(valeur: str) -> str:
    """Échappe une valeur de champ chaîne pour le format Line Protocol (guillemets)."""
    return str(valeur).replace("\\", "\\\\").replace('"', '\\"')


def flux_escape(valeur: str) -> str:
    """Échappe une chaîne pour l'insérer dans un filtre Flux (r.tag == "...")."""
    return str(valeur).replace("\\", "\\\\").replace('"', '\\"')


def to_rfc3339(dt: datetime) -> str:
    """Formate un datetime en RFC3339/UTC tel qu'attendu par les littéraux temporels Flux."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
