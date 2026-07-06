"""
Consommateur Kafka → InfluxDB — VPS cloud, MurMetric / FRD-CODEM.

Consomme les topics Kafka MurMetric et écrit les points dans InfluxDB
en mode batch asynchrone, adapté au débit élevé des capteurs de retrait
DeweSoftX (1 mesure/s par canal).

Topics consommés (namespaced par tenant) :
    murmetric.{tenant}.capteurs.bruts    → mesure : mesures_capteurs
    murmetric.{tenant}.capteurs.registre → mesure : registre_capteurs
    murmetric.{tenant}.dewesoft.bruts    → mesure : mesures_dewesoft

Mode d'écriture InfluxDB :
    Batch asynchrone — flush toutes les 1 s ou dès 500 points accumulés.
    Adapté à l'ingestion continue à haute fréquence sans bloquer la
    consommation Kafka entre deux écritures.

Pourquoi un consommateur séparé du bridge ?
    - Indépendance : un futur consommateur "alertes" peut lire les mêmes
      topics Kafka sans modifier ce script.
    - Reprise sur panne : si InfluxDB redémarre, le consumer group Kafka
      reprend depuis le dernier offset committé, sans perte de données.
    - Scalabilité : plusieurs instances de ce script peuvent tourner en
      parallèle (même group_id) pour répartir la charge.

Usage :
    python kafka_consumer_influx.py

    Variables d'environnement :
        KAFKA_BOOTSTRAP   Serveurs Kafka             (défaut : localhost:9092)
        KAFKA_GROUP_ID    Consumer group ID          (défaut : murmetric-influx)
        TENANT_ID         Identifiant tenant         (défaut : frd)
        INFLUX_URL        URL InfluxDB               (défaut : http://localhost:8086)
        INFLUX_TOKEN      Token API InfluxDB
        INFLUX_ORG        Organisation InfluxDB      (défaut : FRD_CODEM)
        INFLUX_BUCKET     Bucket de destination      (défaut : Test_Capteurs)
"""

import json
import os
import sys

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WriteOptions
from kafka import KafkaConsumer

sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Configuration Kafka.
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "murmetric-influx")
TENANT_ID = os.getenv("TENANT_ID", "frd")

# ---------------------------------------------------------------------------
# Configuration InfluxDB.
# ---------------------------------------------------------------------------
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "MON_TOKEN_API_GENERE_PAR_INFLUXDB")
INFLUX_ORG = os.getenv("INFLUX_ORG", "FRD_CODEM")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Test_Capteurs")

# Noms des mesures InfluxDB.
MESURE_CAPTEURS = "mesures_capteurs"
MESURE_REGISTRE = "registre_capteurs"
MESURE_DEWESOFT = "mesures_dewesoft"

# Topics Kafka à consommer (namespaced par tenant).
TOPICS = [
    f"murmetric.{TENANT_ID}.capteurs.bruts",
    f"murmetric.{TENANT_ID}.capteurs.registre",
    f"murmetric.{TENANT_ID}.dewesoft.bruts",
]


# ---------------------------------------------------------------------------
# Constructeurs de points InfluxDB.
# ---------------------------------------------------------------------------

def construire_point_capteurs(data: dict) -> Point | None:
    """Construire un point InfluxDB pour les mesures BLE (T°/HR%).

    Args:
        data: Payload JSON du topic ``murmetric.*.capteurs.bruts``.

    Returns:
        Point InfluxDB ou None si les mesures principales sont invalides.
    """
    temperature = data.get("temperature_c")
    humidite = data.get("humidite_percent")

    if not isinstance(temperature, (int, float)) or not isinstance(
        humidite, (int, float)
    ):
        return None

    point = (
        Point(MESURE_CAPTEURS)
        .tag("adresse_mac", data.get("mac", "inconnu"))
        .tag("nom_capteur", data.get("capteur_id", "Inconnu"))
        .tag("emplacement", data.get("emplacement") or "Non défini")
        .field("temperature", float(temperature))
        .field("humidite", float(humidite))
    )

    point_de_rosee = data.get("point_de_rosee_c")
    if isinstance(point_de_rosee, (int, float)):
        point = point.field("point_de_rosee", float(point_de_rosee))

    batterie = data.get("batterie_percent")
    if isinstance(batterie, (int, float)):
        point = point.field("batterie", int(batterie))

    rssi = data.get("rssi_dbm")
    if isinstance(rssi, (int, float)):
        point = point.field("rssi", int(rssi))

    return point


def construire_point_registre(data: dict) -> Point | None:
    """Construire un point InfluxDB pour le registre de configuration capteurs.

    Args:
        data: Payload JSON du topic ``murmetric.*.capteurs.registre``.

    Returns:
        Point InfluxDB ou None si la MAC est absente.
    """
    mac = data.get("mac", "").strip()
    if not mac:
        return None

    point = (
        Point(MESURE_REGISTRE)
        .tag("adresse_mac", mac)
        .tag("nom", data.get("nom") or "Non défini")
        .tag("emplacement", data.get("emplacement") or "Non défini")
        .tag("prestation", data.get("prestation") or "Non défini")
        .tag("rd", data.get("categorie R&D") or "Non défini")
        .field("ingestion", bool(data.get("ingestion", False)))
        .field("lint_configure", bool(data.get("lint_configure", False)))
    )

    lint_max = data.get("lint_max_confirme_s")
    if isinstance(lint_max, (int, float)):
        point = point.field("lint_max_confirme_s", float(lint_max))

    for champ in ("latitude", "longitude", "altitude_m"):
        valeur = data.get(champ)
        if isinstance(valeur, (int, float)):
            point = point.field(champ, float(valeur))

    return point


def construire_point_dewesoft(data: dict) -> Point | None:
    """Construire un point InfluxDB pour les mesures DeweSoftX (retrait).

    Structure du payload (depuis ingestion_dewesoft.py) :
        source, canal_index, canal_nom, canal_unite, valeur,
        horodatage_dewesoft, horodatage, taux_echantillonnage.

    Args:
        data: Payload JSON du topic ``murmetric.*.dewesoft.bruts``.

    Returns:
        Point InfluxDB ou None si la valeur de mesure est invalide.
    """
    valeur = data.get("valeur")
    if not isinstance(valeur, (int, float)):
        return None

    point = (
        Point(MESURE_DEWESOFT)
        .tag("source", data.get("source", "dewesoft"))
        .tag("canal_nom", data.get("canal_nom", "inconnu"))
        .tag("canal_unite", data.get("canal_unite", ""))
        .field("valeur", float(valeur))
        .field("canal_index", int(data.get("canal_index", 0)))
    )

    taux = data.get("taux_echantillonnage")
    if isinstance(taux, (int, float)):
        point = point.field("taux_echantillonnage", float(taux))

    return point


# ---------------------------------------------------------------------------
# Router : topic Kafka → constructeur de point InfluxDB.
# ---------------------------------------------------------------------------

CONSTRUCTEURS = {
    f"murmetric.{TENANT_ID}.capteurs.bruts":    construire_point_capteurs,
    f"murmetric.{TENANT_ID}.capteurs.registre": construire_point_registre,
    f"murmetric.{TENANT_ID}.dewesoft.bruts":    construire_point_dewesoft,
}

# ---------------------------------------------------------------------------
# Initialisation InfluxDB — mode batch asynchrone.
# ---------------------------------------------------------------------------

print("⏳ Connexion à InfluxDB...")
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(
    write_options=WriteOptions(
        # Flush dès 500 points accumulés ou toutes les 1 s.
        # Adapté au débit DeweSoft (1 msg/s) sans écriture point par point.
        batch_size=500,
        flush_interval=1_000,
        jitter_interval=0,
        retry_interval=5_000,
        max_retries=3,
        max_retry_delay=30_000,
        exponential_base=2,
    )
)
print(f"✅ InfluxDB prêt ({INFLUX_URL})")

# ---------------------------------------------------------------------------
# Initialisation consommateur Kafka.
# ---------------------------------------------------------------------------

print(f"⏳ Connexion à Kafka ({KAFKA_BOOTSTRAP})...")
consommateur = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=KAFKA_GROUP_ID,
    # earliest : reprendre depuis le début si ce consumer group est nouveau.
    # Garantit qu'aucun message historique Kafka n'est perdu au démarrage.
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)
print(f"✅ Kafka prêt — group_id : {KAFKA_GROUP_ID}")
print(f"   Topics : {', '.join(TOPICS)}\n")

# ---------------------------------------------------------------------------
# Boucle de consommation.
# ---------------------------------------------------------------------------

try:
    for message in consommateur:
        topic = message.topic
        data = message.value

        constructeur = CONSTRUCTEURS.get(topic)
        if constructeur is None:
            continue

        point = constructeur(data)
        if point is None:
            continue

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

        # Affichage condensé : type de mesure uniquement pour ne pas saturer
        # le terminal à 1 msg/s (DeweSoft).
        type_mesure = topic.split(".")[-2]
        print(f"💾 [{type_mesure}] → InfluxDB")

except KeyboardInterrupt:
    print("\n🛑 Arrêt demandé.")
finally:
    write_api.close()
    influx_client.close()
    consommateur.close()
    print("👋 Consommateur Kafka → InfluxDB arrêté.")
