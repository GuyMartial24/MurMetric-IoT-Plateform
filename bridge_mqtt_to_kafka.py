"""
Bridge MQTT → Kafka — VPS cloud, MurMetric / FRD-CODEM.

Souscrit aux topics MQTT publiés par les ingestions (RPi + PC Windows)
et transfère les messages bruts dans les topics Kafka correspondants,
découplant les producteurs (RPi, PC) des consommateurs (InfluxDB, alertes…).

Mapping topics MQTT → Kafka :
    frd/capteurs/bruts      → murmetric.{tenant}.capteurs.bruts
    frd/capteurs/registre   → murmetric.{tenant}.capteurs.registre
    frd/dewesoft/bruts      → murmetric.{tenant}.dewesoft.bruts

Les messages sont transférés tels quels (JSON brut) sans transformation.
La transformation et l'écriture dans InfluxDB sont déléguées au consommateur
``kafka_consumer_influx.py``, permettant d'ajouter d'autres consommateurs
indépendants (alertes, ML, export…) sans modifier ce bridge.

Pourquoi Kafka ?
    - Persistance : les messages survivent à un redémarrage d'InfluxDB.
    - Découplage : chaque consommateur lit à son propre rythme.
    - Multi-tenant : un topic par client (murmetric.{tenant}.*).
    - Débit : absorbe les 1 msg/s des capteurs de retrait DeweSoftX
      sans saturer le consommateur InfluxDB.

Usage :
    python bridge_mqtt_to_kafka.py

    Variables d'environnement :
        MQTT_BROKER     Adresse du broker MQTT    (défaut : localhost)
        MQTT_PORT       Port MQTT                 (défaut : 1883)
        KAFKA_BOOTSTRAP Serveurs Kafka            (défaut : localhost:9092)
        TENANT_ID       Identifiant tenant SaaS   (défaut : frd)
"""

import json
import os
import sys

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.stdout.reconfigure(encoding="utf-8")

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TENANT_ID = os.getenv("TENANT_ID", "frd")

# Mapping topic MQTT → topic Kafka (namespaced par tenant pour le SaaS).
TOPIC_MAP: dict[str, str] = {
    "frd/capteurs/bruts":    f"murmetric.{TENANT_ID}.capteurs.bruts",
    "frd/capteurs/registre": f"murmetric.{TENANT_ID}.capteurs.registre",
    "frd/dewesoft/bruts":    f"murmetric.{TENANT_ID}.dewesoft.bruts",
}


def _on_kafka_erreur(exc: KafkaError) -> None:
    """Callback appelé si un envoi Kafka échoue de manière asynchrone."""
    print(f"❌ Kafka — erreur d'envoi : {exc}")


print("⏳ Connexion à Kafka...")
producteur = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    # acks=1 : attendre confirmation d'au moins 1 broker.
    # Équilibre fiabilité/débit — passer à acks="all" si réplication activée.
    acks=1,
    retries=3,
    retry_backoff_ms=500,
)
print(f"✅ Kafka prêt ({KAFKA_BOOTSTRAP})")


def on_message(client, userdata, msg) -> None:
    """Transférer un message MQTT vers le topic Kafka correspondant.

    Args:
        client:   Instance du client MQTT (non utilisée ici).
        userdata: Données utilisateur optionnelles (non utilisées).
        msg:      Message MQTT reçu (topic + payload JSON).
    """
    topic_kafka = TOPIC_MAP.get(msg.topic)
    if topic_kafka is None:
        return

    try:
        data = json.loads(msg.payload.decode("utf-8"))
        producteur.send(topic_kafka, value=data).add_errback(_on_kafka_erreur)
        print(f"📨  {msg.topic}  →  {topic_kafka}")
    except (json.JSONDecodeError, Exception) as exc:
        print(f"❌ Erreur transfert [{msg.topic}] : {exc}")


def on_connect(client, userdata, flags, rc) -> None:
    """Souscrire à tous les topics MQTT dès la connexion établie.

    Args:
        client:   Instance du client MQTT.
        userdata: Données utilisateur optionnelles (non utilisées).
        flags:    Drapeaux de réponse du broker.
        rc:       Code de retour (0 = succès).
    """
    if rc == 0:
        souscriptions = [(topic, 1) for topic in TOPIC_MAP]
        client.subscribe(souscriptions)
        print(
            f"📥 MQTT connecté ({MQTT_BROKER}) — "
            f"{len(souscriptions)} topic(s) écoutés"
        )
    else:
        print(f"❌ MQTT — connexion refusée (code {rc})")


mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

print(f"⏳ Connexion à Mosquitto ({MQTT_BROKER})...")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

try:
    mqtt_client.loop_forever()
finally:
    producteur.flush()
    producteur.close()
    print("👋 Bridge MQTT → Kafka arrêté.")
