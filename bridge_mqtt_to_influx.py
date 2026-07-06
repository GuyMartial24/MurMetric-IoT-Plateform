"""
Bridge MQTT → InfluxDB pour les capteurs BLE Blue Maestro — FRD-CODEM.

Ce script souscrit au topic MQTT ``frd/capteurs/bruts`` publié par
``test_ingestion.py`` et écrit chaque mesure dans InfluxDB (bucket
``Test_Capteurs``, organisation ``FRD_CODEM``).

Modèle de données InfluxDB (mesure : ``mesures_capteurs``) :

    Tags (string, indexés — utilisés pour filtrer/grouper dans Grafana) :
        adresse_mac  : adresse BLE du capteur (XX:XX:XX:XX:XX:XX)
        nom_capteur  : identifiant lisible (nom défini dans capteurs.json)
        emplacement  : localisation physique (depuis capteurs.json)

    Fields (numériques, stockés en séries temporelles) :
        temperature      (°C, float)
        humidite         (%, float)
        point_de_rosee   (°C, float)  — None si non calculable
        batterie         (%, int)     — None si non disponible
        rssi             (dBm, int)   — intensité signal BLE à la réception

Usage :
    python -u bridge_mqtt_to_influx.py

    Variables d'environnement reconnues :
        INFLUX_URL    URL d'InfluxDB              (défaut : http://localhost:8086)
        INFLUX_TOKEN  Token API InfluxDB          (défaut : MON_TOKEN_API_GENERE_PAR_INFLUXDB)
        INFLUX_ORG    Organisation InfluxDB       (défaut : FRD_CODEM)
        INFLUX_BUCKET Bucket de destination       (défaut : Test_Capteurs)
        MQTT_BROKER   Adresse du broker MQTT      (défaut : localhost)
        MQTT_PORT     Port du broker MQTT         (défaut : 1883)
        MQTT_TOPIC    Topic MQTT à écouter        (défaut : frd/capteurs/bruts)
"""

import json
import os
import sys

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Requis pour afficher les emojis sous Windows (console cp1252 par défaut).
sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Configuration InfluxDB — surchargeable via variables d'environnement.
# ---------------------------------------------------------------------------
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "MON_TOKEN_API_GENERE_PAR_INFLUXDB")
INFLUX_ORG = os.getenv("INFLUX_ORG", "FRD_CODEM")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Test_Capteurs")

# Nom de la mesure InfluxDB pour les données de capteurs (time-series).
MESURE = "mesures_capteurs"

# Nom de la mesure InfluxDB pour le registre de configuration des capteurs.
# Contient les métadonnées (nom, emplacement, ingestion…) de chaque capteur.
# Une requête last() par adresse_mac retourne la config actuelle d'un capteur.
MESURE_REGISTRE = "registre_capteurs"

# ---------------------------------------------------------------------------
# Configuration MQTT — surchargeable via variables d'environnement.
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "frd/capteurs/bruts")
MQTT_TOPIC_REGISTRE = os.getenv("MQTT_TOPIC_REGISTRE", "frd/capteurs/registre")

# ---------------------------------------------------------------------------
# Initialisation des clients InfluxDB et MQTT.
# ---------------------------------------------------------------------------
print("⏳ Connexion à InfluxDB...")
influx_client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)


def construire_point(data: dict) -> Point | None:
    """Construire un point InfluxDB à partir d'un message MQTT décodé.

    Seuls les champs numériques valides sont ajoutés : les valeurs None ou
    non numériques (chaînes d'erreur comme "Trame tronquée") sont ignorées
    pour ne pas corrompre la série temporelle.

    Args:
        data: Dictionnaire issu du JSON MQTT publié par test_ingestion.py.

    Returns:
        Un objet Point InfluxDB prêt à l'écriture, ou None si les mesures
        principales (température et humidité) sont invalides.
    """
    temperature = data.get("temperature_c")
    humidite = data.get("humidite_percent")

    # Les mesures principales doivent être numériques pour que le point
    # soit utile. Les autres champs sont ajoutés s'ils sont disponibles.
    if not isinstance(temperature, (int, float)) or not isinstance(
        humidite, (int, float)
    ):
        return None

    mac = data.get("mac", "inconnu")
    nom = data.get("capteur_id", "Inconnu")
    emplacement = data.get("emplacement") or "Non défini"

    point = (
        Point(MESURE)
        # Tags : métadonnées string pour le filtrage et le groupement.
        .tag("adresse_mac", mac)
        .tag("nom_capteur", nom)
        .tag("emplacement", emplacement)
        # Fields principaux.
        .field("temperature", float(temperature))
        .field("humidite", float(humidite))
    )

    # Champs optionnels — ajoutés uniquement si numériques.
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

    Les métadonnées de configuration (nom, emplacement, flags GATT…) sont
    stockées dans la mesure ``registre_capteurs``. Une requête ``last()`` par
    ``adresse_mac`` retourne toujours la configuration la plus récente d'un
    capteur, quelle que soit l'application cliente.

    Les champs string (nom, emplacement) sont convertis en tags pour permettre
    le filtrage Grafana. Les flags booléens et numériques sont stockés en fields.

    Args:
        data: Dictionnaire issu du topic ``frd/capteurs/registre``.

    Returns:
        Un objet Point InfluxDB, ou None si la MAC est absente/invalide.
    """
    mac = data.get("mac", "").strip()
    if not mac:
        return None

    point = (
        Point(MESURE_REGISTRE)
        # Tag principal : identifiant unique du capteur.
        .tag("adresse_mac", mac)
        # Tags string : permettent le filtrage dans Grafana.
        .tag("nom", data.get("nom") or "Non défini")
        .tag("emplacement", data.get("emplacement") or "Non défini")
        .tag("prestation", data.get("prestation") or "Non défini")
        .tag("rd", data.get("categorie R&D") or "Non défini")
        # Fields booléens et numériques.
        .field("ingestion", bool(data.get("ingestion", False)))
        .field("lint_configure", bool(data.get("lint_configure", False)))
    )

    lint_max = data.get("lint_max_confirme_s")
    if isinstance(lint_max, (int, float)):
        point = point.field("lint_max_confirme_s", float(lint_max))

    # Coordonnées GPS — stockées uniquement si renseignées (non null).
    for champ in ("latitude", "longitude", "altitude_m"):
        valeur = data.get(champ)
        if isinstance(valeur, (int, float)):
            point = point.field(champ, float(valeur))

    return point


def on_message(client, userdata, msg) -> None:
    """Router un message MQTT reçu vers l'handler InfluxDB approprié.

    Deux topics sont écoutés :
    - ``frd/capteurs/bruts``   → mesures time-series (``mesures_capteurs``)
    - ``frd/capteurs/registre`` → métadonnées capteurs (``registre_capteurs``)

    Args:
        client:   Instance du client MQTT (non utilisée ici).
        userdata: Données utilisateur optionnelles (non utilisées).
        msg:      Message MQTT reçu (topic + payload).
    """
    try:
        data = json.loads(msg.payload.decode("utf-8"))

        if msg.topic == MQTT_TOPIC_REGISTRE:
            # --- Registre de configuration des capteurs ---
            point = construire_point_registre(data)
            if point is None:
                return
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(
                f"📋 [InfluxDB registre] {data.get('mac', '?')} — "
                f"nom: \"{data.get('nom', '')}\" | "
                f"emplacement: \"{data.get('emplacement', '')}\" | "
                f"ingestion: {data.get('ingestion', False)}"
            )
        else:
            # --- Mesures time-series des capteurs ---
            point = construire_point(data)
            if point is None:
                # Mesures invalides (chaîne d'erreur ou None) : on ignore.
                return
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            mac = data.get("mac", "?")
            temp = data.get("temperature_c")
            hum = data.get("humidite_percent")
            print(
                f"💾 [InfluxDB mesures] {data.get('capteur_id', mac)} "
                f"({data.get('emplacement', '')}) — "
                f"T°: {temp} °C | HR: {hum} %"
            )

    except Exception as exc:
        print(f"❌ Erreur lors de l'écriture dans InfluxDB : {exc}")


def on_connect(client, userdata, flags, rc) -> None:
    """Callback de connexion MQTT — souscrire au topic dès la connexion établie.

    Args:
        client:   Instance du client MQTT.
        userdata: Données utilisateur optionnelles (non utilisées).
        flags:    Drapeaux de réponse du broker (non utilisés).
        rc:       Code de retour de connexion (0 = succès).
    """
    if rc == 0:
        print(
            f"📥 Connecté à Mosquitto — écoute des topics :\n"
            f"    • {MQTT_TOPIC} (mesures)\n"
            f"    • {MQTT_TOPIC_REGISTRE} (registre capteurs)"
        )
        # Souscription aux deux topics avec QoS 1.
        client.subscribe([(MQTT_TOPIC, 1), (MQTT_TOPIC_REGISTRE, 1)])
    else:
        print(f"❌ Connexion MQTT refusée (code {rc})")


mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

try:
    print("📥 Connexion à Mosquitto...")
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_forever()
finally:
    try:
        write_api.close()
    finally:
        influx_client.close()

