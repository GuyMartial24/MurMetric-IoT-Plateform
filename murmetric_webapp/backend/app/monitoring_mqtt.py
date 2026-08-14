"""Abonné MQTT léger — battements de vie des pipelines d'ingestion (section 32,
13/08/2026). ingestion_dewesoft_dxd.py (PC Amiens) et
ingestion_capteurs_bluetooth.py (Pi) publient périodiquement un petit message
JSON sur MQTT_TOPIC_HEARTBEAT ; ce module s'y abonne côté broker **interne**
au cluster (mosquitto:1883, pas le 8883/TLS externe) et écrit chaque
battement dans InfluxDB (mesure "pipeline_heartbeat") pour que la page
Monitoring de la webapp puisse en afficher l'état et l'évolution.

Volontairement en dehors du chemin Kafka (bridge-mqtt-kafka /
kafka-consumer-influx) : ce chemin existe pour la fiabilité/le volume des
mesures réelles, disproportionné pour un signal de supervision à faible
fréquence (quelques messages/minute) et à faible enjeu (un battement perdu
n'a aucune conséquence, contrairement à un point de mesure)."""
import json
import threading

import paho.mqtt.client as mqtt

from . import config
from .influx import MESURE_HEARTBEAT, echap_field_str, echap_tag, write_point

_client: mqtt.Client | None = None


def _traiter_message(payload: dict) -> None:
    pipeline = echap_tag(payload.get("pipeline") or "inconnu")
    machine = echap_tag(payload.get("machine") or "inconnu")
    champs = ",".join([
        f"mqtt_connecte={1 if payload.get('mqtt_connecte') else 0}i",
        f"buffer_sqlite_en_attente={int(payload.get('buffer_sqlite_en_attente') or 0)}i",
        f"registre_api_ok={1 if payload.get('registre_api_ok') else 0}i",
        f"nb_capteurs_connus={int(payload.get('nb_capteurs_connus') or 0)}i",
        f'demarre_le="{echap_field_str(payload.get("demarre_le") or "")}"',
        f"nb_points_publies={int(payload.get('nb_points_publies') or 0)}i",
        f"nb_points_bufferises={int(payload.get('nb_points_bufferises') or 0)}i",
    ])
    write_point(f"{MESURE_HEARTBEAT},pipeline={pipeline},machine={machine} {champs}")


def _on_message(_client, _userdata, message) -> None:
    try:
        payload = json.loads(message.payload.decode("utf-8"))
        _traiter_message(payload)
    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        print(f"⚠️ Battement de vie ignoré (payload invalide) : {exc}")


def demarrer() -> None:
    """Connecter le client MQTT en arrière-plan (thread paho dédié, non
    bloquant) — no-op silencieux si aucun broker n'est configuré (dev local
    sans MQTT_BROKER défini)."""
    global _client
    if not config.MQTT_BROKER or config.MQTT_BROKER == "localhost":
        return

    client = mqtt.Client()
    if config.MQTT_USERNAME:
        client.username_pw_set(config.MQTT_USERNAME, config.MQTT_PASSWORD)
    client.on_message = _on_message

    def _on_connect(c, _userdata, _flags, rc) -> None:
        if rc == 0:
            c.subscribe(config.MQTT_TOPIC_HEARTBEAT, qos=1)
            print(f"✅ Monitoring MQTT connecté, abonné à {config.MQTT_TOPIC_HEARTBEAT}")
        else:
            print(f"⚠️ Monitoring MQTT — connexion refusée (code {rc})")

    client.on_connect = _on_connect

    def _connecter() -> None:
        try:
            client.connect(config.MQTT_BROKER, config.MQTT_PORT, keepalive=60)
            client.loop_start()
        except OSError as exc:
            print(f"⚠️ Monitoring MQTT injoignable au démarrage ({exc}) — pas de heartbeat live.")

    # Non-bloquant : un broker interne indisponible au démarrage de la webapp
    # ne doit jamais empêcher celle-ci de démarrer (le reste de l'appli ne
    # dépend pas de ce flux).
    threading.Thread(target=_connecter, daemon=True).start()
    _client = client


def arreter() -> None:
    if _client is not None:
        _client.loop_stop()
        _client.disconnect()
