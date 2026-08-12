"""Client InfluxDB partagé + helpers de requêtage/écriture pour l'API."""
from datetime import datetime, timezone
from functools import lru_cache

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from . import config

MESURE_CAPTEURS = "mesures_capteurs"
MESURE_DEWESOFT = "mesures_dewesoft"
MESURE_TENEUR_EAU = "mesures_teneur_eau"


@lru_cache
def get_client() -> InfluxDBClient:
    # Timeout par défaut du client (10s) trop court pour une agrégation sur
    # mesures_dewesoft (retrait, ~1,5 milliard de points, échantillonnage
    # 100 Hz) — vérifié en conditions réelles le 12/08/2026 (read timeout à
    # 10s sur une requête aggregateWindow(1d) filtrée par canal seul, sur un
    # an). 30s laisse la marge nécessaire ; un filtre mur/couche/canal
    # réduit généralement bien plus vite en pratique.
    return InfluxDBClient(url=config.INFLUX_URL, token=config.INFLUX_TOKEN, org=config.INFLUX_ORG, timeout=30_000)


def query_api():
    return get_client().query_api()


def write_point(line: str) -> None:
    write_api = get_client().write_api(write_options=SYNCHRONOUS)
    try:
        write_api.write(bucket=config.INFLUX_BUCKET, org=config.INFLUX_ORG, record=line, write_precision=WritePrecision.NS)
    finally:
        write_api.close()


def delete_points(predicate: str, start: datetime, stop: datetime) -> None:
    get_client().delete_api().delete(start, stop, predicate, bucket=config.INFLUX_BUCKET, org=config.INFLUX_ORG)


def echap_tag(valeur: str) -> str:
    return str(valeur).replace("\\", "\\\\").replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def echap_field_str(valeur: str) -> str:
    return str(valeur).replace("\\", "\\\\").replace('"', '\\"')


def flux_escape(valeur: str) -> str:
    """Échappe une chaîne pour l'insérer dans un filtre Flux (r.tag == "...")."""
    return str(valeur).replace("\\", "\\\\").replace('"', '\\"')


def to_rfc3339(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
