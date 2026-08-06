"""
Consommateur Kafka → InfluxDB — VPS cloud, MurMetric / FRD-CODEM.

Consomme les topics Kafka MurMetric et écrit les points dans InfluxDB
en mode batch asynchrone, adapté au débit élevé des capteurs de retrait
DeweSoftX (1 mesure/s par canal).

Topics consommés (namespaced par tenant) :
    murmetric.{tenant}.capteurs.bruts    → mesure : mesures_capteurs
    murmetric.{tenant}.capteurs.registre → mesure : registre_capteurs
    murmetric.{tenant}.dewesoft.bruts    → mesure : mesures_dewesoft
    murmetric.{tenant}.dewesoft.alertes  → mesure : alertes_ingestion

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
        INFLUX_BUCKET     Bucket de destination      (défaut : Capteurs)
"""

import json
import os
import sys
import time
from datetime import datetime

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
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Capteurs")

# Noms des mesures InfluxDB.
MESURE_CAPTEURS = "mesures_capteurs"
MESURE_REGISTRE = "registre_capteurs"
MESURE_DEWESOFT = "mesures_dewesoft"
MESURE_ALERTES = "alertes_ingestion"

# Topics Kafka à consommer (namespaced par tenant).
TOPICS = [
    f"murmetric.{TENANT_ID}.capteurs.bruts",
    f"murmetric.{TENANT_ID}.capteurs.registre",
    f"murmetric.{TENANT_ID}.dewesoft.bruts",
    f"murmetric.{TENANT_ID}.dewesoft.alertes",
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
        .tag("nom_mur", data.get("nom_mur") or "Non défini")
        .tag("nom_couche", data.get("nom_couche") or "Non défini")
        .tag("position", data.get("position") or "Non défini")
        .tag("prestation", data.get("prestation") or "Non défini")
        .tag("rd", data.get("categorie R&D") or "Non défini")
        .field("ingestion", bool(data.get("ingestion", False)))
        .field("lint_configure", bool(data.get("lint_configure", False)))
    )

    lint_max = data.get("lint_max_confirme_s")
    if isinstance(lint_max, (int, float)):
        point = point.field("lint_max_confirme_s", float(lint_max))

    return point


def construire_point_dewesoft(data: dict) -> Point | None:
    """Construire un point InfluxDB pour les mesures DeweSoftX (retrait).

    Structure du payload (depuis ingestion_dewesoft_dxd.py, import .dxd) :
        source, canal_index, canal_nom, canal_unite, valeur,
        horodatage_dewesoft, horodatage, taux_echantillonnage,
        horodatage_mesure_iso — horodatage réel de la mesure (reconstitué à
        partir du début d'enregistrement du fichier .dxd), utilisé pour dater
        le point (_time) plutôt que l'instant d'écriture Kafka/InfluxDB, afin
        de ne pas écraser l'historique lors d'un import différé. Republié en
        plus comme field "horodatage_lisible" (JJ/MM/AAAA HH:MM:SS) pour
        lecture directe dans un export brut, sans reparser _time.

        valeur_filtree / est_aberrant — sortie du filtre de Hampel anti-
        vibration appliqué à l'ingestion (cf. filtrer_hampel() dans
        ingestion_dewesoft_dxd.py), avec un seuil par défaut. La valeur brute
        (field "valeur") n'est jamais modifiée : les deux sont conservées
        pour permettre une visualisation brut/filtré côte à côte côté appli.

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
        .tag("nom_mur", data.get("nom_mur") or "Non défini")
        .tag("nom_couche", data.get("nom_couche") or "Non défini")
        .tag("position", data.get("position") or "Non défini")
        .tag("rd", data.get("categorie R&D") or "Non défini")
        .field("valeur", float(valeur))
        .field("canal_index", int(data.get("canal_index", 0)))
    )

    taux = data.get("taux_echantillonnage")
    if isinstance(taux, (int, float)):
        point = point.field("taux_echantillonnage", float(taux))

    valeur_filtree = data.get("valeur_filtree")
    if isinstance(valeur_filtree, (int, float)):
        point = point.field("valeur_filtree", float(valeur_filtree))

    est_aberrant = data.get("est_aberrant")
    if isinstance(est_aberrant, bool):
        point = point.field("est_aberrant", est_aberrant)

    # Import .dxd : dater le point avec l'horodatage réel de la mesure plutôt
    # que l'instant d'écriture — sinon un import différé écraserait tout
    # l'historique sur la date d'exécution du consumer.
    horodatage_mesure_iso = data.get("horodatage_mesure_iso")
    if isinstance(horodatage_mesure_iso, str):
        try:
            horodatage_mesure = datetime.fromisoformat(horodatage_mesure_iso)
            point = point.time(horodatage_mesure)
            point = point.field(
                "horodatage_lisible", horodatage_mesure.strftime("%d/%m/%Y %H:%M:%S")
            )
        except ValueError:
            pass

    return point


def construire_point_alerte(data: dict) -> Point | None:
    """Construire un point InfluxDB pour une anomalie d'ingestion.

    Aucune journalisation persistante n'existe ailleurs dans le pipeline
    (tout part sur stdout, perdu si personne ne regarde) — publier chaque
    anomalie comme point InfluxDB la rend persistante, horodatée,
    interrogeable en Flux et visible dans Grafana sans outil supplémentaire
    (cf. logique_projet.md section 19). Premier cas d'usage : collision de
    nom de canal DeweSoft (deux canaux de même nom dans un même fichier .dxd).

    Args:
        data: Payload JSON du topic ``murmetric.*.dewesoft.alertes``.

    Returns:
        Point InfluxDB, ou None si le type d'anomalie est absent.
    """
    type_alerte = data.get("type")
    if not type_alerte:
        return None

    point = (
        Point(MESURE_ALERTES)
        .tag("type", type_alerte)
        .tag("canal_nom", data.get("canal_nom", "inconnu"))
        .tag("fichier_source", data.get("fichier_source", "inconnu"))
        .field("occurrences", int(data.get("occurrences", 0)))
    )

    return point


# ---------------------------------------------------------------------------
# Router : topic Kafka → constructeur de point InfluxDB.
# ---------------------------------------------------------------------------

CONSTRUCTEURS = {
    f"murmetric.{TENANT_ID}.capteurs.bruts":    construire_point_capteurs,
    f"murmetric.{TENANT_ID}.capteurs.registre": construire_point_registre,
    f"murmetric.{TENANT_ID}.dewesoft.bruts":    construire_point_dewesoft,
    f"murmetric.{TENANT_ID}.dewesoft.alertes":  construire_point_alerte,
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

def _connecter_consommateur_kafka() -> KafkaConsumer:
    """Créer le consumer Kafka, en réessayant tant que le bootstrap échoue.

    KafkaConsumer() se connecte au cluster dès sa construction et lève une
    exception immédiate si Kafka est injoignable — sans ce ré-essai, le
    conteneur planterait au moindre léger décalage de démarrage entre
    services (ex. Kubernetes, où l'ordre de disponibilité des pods n'est pas
    garanti malgré depends_on/initContainers).
    """
    while True:
        try:
            return KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP_ID,
                # earliest : reprendre depuis le début si ce consumer group est nouveau.
                # Garantit qu'aucun message historique Kafka n'est perdu au démarrage.
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
        except Exception as exc:
            print(f"⚠️  Kafka injoignable ({exc}) — nouvelle tentative dans 5 s...")
            time.sleep(5)


print(f"⏳ Connexion à Kafka ({KAFKA_BOOTSTRAP})...")
consommateur = _connecter_consommateur_kafka()
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
