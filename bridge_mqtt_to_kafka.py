"""
Bridge MQTT → Kafka — VPS cloud, MurMetric / FRD-CODEM.

Souscrit aux topics MQTT publiés par les ingestions (RPi + PC Windows)
et transfère les messages bruts dans les topics Kafka correspondants,
découplant les producteurs (RPi, PC) des consommateurs (InfluxDB, alertes…).

Mapping topics MQTT → Kafka :
    frd/capteurs/bruts      → murmetric.{tenant}.capteurs.bruts
    frd/capteurs/registre   → murmetric.{tenant}.capteurs.registre
    frd/dewesoft/bruts      → murmetric.{tenant}.dewesoft.bruts
    frd/dewesoft/alertes    → murmetric.{tenant}.dewesoft.alertes

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
        MQTT_USERNAME   Utilisateur MQTT          (défaut : vide, pas d'auth tentée)
        MQTT_PASSWORD   Mot de passe MQTT         (défaut : vide)
        KAFKA_BOOTSTRAP Serveurs Kafka            (défaut : localhost:9092)
        TENANT_ID       Identifiant tenant SaaS   (défaut : frd)
        KAFKA_LINGER_MS Regroupement des envois Kafka, en ms (défaut : 20)
        KAFKA_BATCH_SIZE     Taille de lot Kafka, en octets  (défaut : 131072)
        KAFKA_COMPRESSION    Compression Kafka (défaut : gzip ; vide = aucune)
        LOG_INTERVAL    Période du récapitulatif de débit, en s (défaut : 10)
        MQTT_SHARE_GROUP     Groupe de souscription partagée MQTT (défaut : bridge)

Parallélisation (07/08/2026, cf. logique_projet.md) :
    Jusqu'ici ce bridge était volontairement limité à 1 replica : plusieurs
    instances abonnées au même topic MQTT reçoivent chacune TOUS les
    messages et les republieraient donc en double dans Kafka. Mosquitto
    (>= 1.6, confirmé ici en 2.0.22) implémente les souscriptions partagées
    ("$share/<groupe>/<topic>") même pour des clients MQTT 3.1.1 classiques
    (ce n'est pas une fonctionnalité MQTT v5 uniquement, malgré une note
    antérieure erronée à ce sujet) : le broker répartit alors les messages
    d'un topic entre tous les clients abonnés au même groupe, sans doublon.
    Ce bridge peut donc désormais tourner en plusieurs replicas (cf.
    k8s/bridge-mqtt-kafka/deployment.yaml) tant qu'ils partagent tous le même
    MQTT_SHARE_GROUP. msg.topic reçu dans on_message() reste le topic réel
    (ex. "frd/dewesoft/bruts"), jamais le préfixe "$share/...", qui n'existe
    que dans la trame SUBSCRIBE — TOPIC_MAP fonctionne donc sans changement.
"""

import os
import sys
import threading
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.stdout.reconfigure(encoding="utf-8")

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
# Authentification MQTT (03/08/2026, cf. logique_projet.md) — Mosquitto
# n'accepte plus les connexions anonymes. Vide = pas d'authentification
# tentée (utile seulement en test local contre un broker encore en
# allow_anonymous ; le mosquitto.conf du projet ne l'autorise plus).
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TENANT_ID = os.getenv("TENANT_ID", "frd")

# ---------------------------------------------------------------------------
# Débit (06/08/2026, cf. logique_projet.md) — réglages introduits après avoir
# mesuré que ce bridge plafonnait à ~1 100 msg/s, ce qui faisait refluer la
# file sortante de Mosquitto jusqu'aux "Outgoing messages are being dropped
# for client ..." lors des imports massifs de .dxd historiques.
#
# Deux causes mesurées, toutes deux dans on_message() qui s'exécute sur le
# THREAD RÉSEAU de paho — tant qu'il n'a pas rendu la main, le bridge ne lit
# plus sa socket MQTT et Mosquitto accumule :
#   1. un print() PAR MESSAGE, non tamponné (les images tournent avec
#      "python -u"), donc un appel système write() par message, relayé sur
#      disque par containerd. Vérifié : 5 095 760 lignes de log du pod pour
#      5 095 751 messages Kafka — exactement une ligne par message.
#   2. un json.loads() + json.dumps() par message, alors que le bridge est
#      censé transférer le JSON "tel quel" : la charge utile MQTT est
#      désormais réexpédiée en octets bruts, sans aucune (dé)sérialisation.
#
# LOG_INTERVAL remplace le print par message par un récapitulatif périodique
# émis depuis un thread dédié (coût nul sur le chemin critique).
# ---------------------------------------------------------------------------
KAFKA_LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "20"))
KAFKA_BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", "131072"))
# gzip divise par ~10 le volume de ces charges utiles JSON très répétitives,
# au prix de CPU côté bridge. Vide = pas de compression.
KAFKA_COMPRESSION = os.getenv("KAFKA_COMPRESSION", "gzip") or None
KAFKA_MAX_REQUEST_SIZE = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", str(1024 * 1024)))
LOG_INTERVAL = float(os.getenv("LOG_INTERVAL", "10"))
# Groupe de souscription partagée — toutes les replicas de ce bridge doivent
# utiliser la même valeur pour se répartir les messages sans doublon.
MQTT_SHARE_GROUP = os.getenv("MQTT_SHARE_GROUP", "bridge")

# Mapping topic MQTT → topic Kafka (namespaced par tenant pour le SaaS).
TOPIC_MAP: dict[str, str] = {
    "frd/capteurs/bruts": f"murmetric.{TENANT_ID}.capteurs.bruts",
    "frd/capteurs/registre": f"murmetric.{TENANT_ID}.capteurs.registre",
    "frd/dewesoft/bruts": f"murmetric.{TENANT_ID}.dewesoft.bruts",
    "frd/dewesoft/alertes": f"murmetric.{TENANT_ID}.dewesoft.alertes",
}


# Compteurs alimentés par on_message() (incréments d'entiers uniquement : le
# GIL les rend suffisamment atomiques ici, et on ne veut aucun verrou sur le
# chemin critique). Lus par la boucle de journalisation périodique.
_nb_transferes = 0
_nb_erreurs = 0
_nb_erreurs_kafka = 0


def _on_kafka_erreur(exc: KafkaError) -> None:
    """Callback appelé si un envoi Kafka échoue de manière asynchrone."""
    global _nb_erreurs_kafka
    _nb_erreurs_kafka += 1
    # Jamais par message : seule la première erreur puis une sur 1000 sont
    # affichées, sinon une panne Kafka regénère le goulot d'étranglement que
    # ce module vient justement de supprimer.
    if _nb_erreurs_kafka == 1 or _nb_erreurs_kafka % 1000 == 0:
        print(f"❌ Kafka — erreur d'envoi (n°{_nb_erreurs_kafka}) : {exc}", flush=True)


def _boucle_journalisation() -> None:
    """Afficher un récapitulatif de débit toutes les LOG_INTERVAL secondes.

    Remplace le print() par message : à 3 456 000 messages par fichier .dxd,
    journaliser chaque transfert coûtait un appel système par message sur le
    thread réseau MQTT (cf. commentaire en tête de module).
    """
    precedent = 0
    while True:
        time.sleep(LOG_INTERVAL)
        total = _nb_transferes
        delta = total - precedent
        precedent = total
        if delta == 0:
            continue
        print(
            f"📨 {delta} message(s) transféré(s) en {LOG_INTERVAL:.0f}s "
            f"({delta / LOG_INTERVAL:.0f}/s) — cumul {total}, "
            f"erreurs {_nb_erreurs + _nb_erreurs_kafka}",
            flush=True,
        )


def _connecter_producteur_kafka() -> KafkaProducer:
    """Créer le producteur Kafka, en réessayant tant que le bootstrap échoue.

    KafkaProducer() se connecte au cluster dès sa construction et lève une
    exception immédiate si Kafka est injoignable — sans ce ré-essai, le
    conteneur planterait au moindre léger décalage de démarrage entre
    services (ex. Kubernetes, où l'ordre de disponibilité des pods n'est pas
    garanti malgré depends_on/initContainers).
    """
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                # Pas de value_serializer : la charge utile MQTT est déjà du
                # JSON encodé en UTF-8 et le bridge la transfère telle quelle.
                # La désérialiser pour la resérialiser aussitôt coûtait deux
                # conversions JSON par message pour un résultat identique.
                # acks=1 : attendre confirmation d'au moins 1 broker.
                # Équilibre fiabilité/débit — passer à acks="all" si réplication activée.
                acks=1,
                retries=3,
                retry_backoff_ms=500,
                # linger_ms : attendre quelques millisecondes que d'autres
                # messages arrivent pour les expédier en un seul lot. Avec le
                # défaut (0), chaque message partait dans sa propre requête
                # réseau vers Kafka — désastreux à 10 Hz × 8 canaux/fichier.
                linger_ms=KAFKA_LINGER_MS,
                batch_size=KAFKA_BATCH_SIZE,
                compression_type=KAFKA_COMPRESSION,
                # Plafond par requête. Explicite depuis le 07/08/2026 : les
                # charges utiles sont passées de ~520 o (un échantillon) à
                # ~17 ko (un lot de 600). On reste très loin du défaut
                # (1 048 576 o), mais le fixer ici documente la contrainte et
                # garde l'alignement avec max.message.bytes côté broker et
                # message_size_limit côté Mosquitto — les trois plafonds de la
                # chaîne valent 1 Mio, donc aucun message accepté en amont ne
                # peut être refusé en aval.
                max_request_size=KAFKA_MAX_REQUEST_SIZE,
            )
        except Exception as exc:
            print(f"⚠️  Kafka injoignable ({exc}) — nouvelle tentative dans 5 s...")
            time.sleep(5)


print("⏳ Connexion à Kafka...")
producteur = _connecter_producteur_kafka()
print(f"✅ Kafka prêt ({KAFKA_BOOTSTRAP})")


def on_message(client, userdata, msg) -> None:
    """Transférer un message MQTT vers le topic Kafka correspondant.

    Args:
        client:   Instance du client MQTT (non utilisée ici).
        userdata: Données utilisateur optionnelles (non utilisées).
        msg:      Message MQTT reçu (topic + payload JSON).

    Ce callback s'exécute sur le thread réseau de paho : tout ce qu'il fait
    retarde d'autant la lecture de la socket MQTT (et donc fait grossir la
    file sortante de Mosquitto). Il doit rester le plus court possible —
    aucune journalisation ni sérialisation par message ici.
    """
    global _nb_transferes, _nb_erreurs

    topic_kafka = TOPIC_MAP.get(msg.topic)
    if topic_kafka is None:
        return

    try:
        # Charge utile transférée en octets bruts, sans (dé)sérialisation.
        producteur.send(topic_kafka, value=msg.payload).add_errback(_on_kafka_erreur)
        _nb_transferes += 1
    except Exception as exc:
        _nb_erreurs += 1
        if _nb_erreurs == 1 or _nb_erreurs % 1000 == 0:
            print(f"❌ Erreur transfert [{msg.topic}] (n°{_nb_erreurs}) : {exc}", flush=True)


def on_connect(client, userdata, flags, rc) -> None:
    """Souscrire à tous les topics MQTT dès la connexion établie.

    Args:
        client:   Instance du client MQTT.
        userdata: Données utilisateur optionnelles (non utilisées).
        flags:    Drapeaux de réponse du broker.
        rc:       Code de retour (0 = succès).
    """
    if rc == 0:
        # $share/<groupe>/<topic> : Mosquitto répartit les messages entre
        # toutes les replicas abonnées au même groupe au lieu de les envoyer
        # à chacune — c'est ce qui permet à ce bridge de tourner en plusieurs
        # exemplaires sans dupliquer les messages vers Kafka.
        souscriptions = [(f"$share/{MQTT_SHARE_GROUP}/{topic}", 1) for topic in TOPIC_MAP]
        client.subscribe(souscriptions)
        print(
            f"📥 MQTT connecté ({MQTT_BROKER}) — "
            f"{len(souscriptions)} topic(s) écoutés (groupe partagé : {MQTT_SHARE_GROUP})"
        )
    else:
        print(f"❌ MQTT — connexion refusée (code {rc})")


mqtt_client = mqtt.Client()
if MQTT_USERNAME:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

print(f"⏳ Connexion à Mosquitto ({MQTT_BROKER})...")
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

threading.Thread(target=_boucle_journalisation, daemon=True).start()

try:
    mqtt_client.loop_forever()
finally:
    producteur.flush()
    producteur.close()
    print("👋 Bridge MQTT → Kafka arrêté.")
