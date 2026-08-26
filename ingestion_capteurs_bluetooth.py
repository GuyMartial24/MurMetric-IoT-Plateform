"""
Ingestion continue des capteurs BLE — MurMetric / FRD-CODEM.

Deux familles de capteurs reconnues (11/08/2026) :
- Blue Maestro (Disc Maxi/Mini, company ID 0x0133).
- ELA Innovation (Blue Puck RHT, company ID 0x0757) — modes "Manufacturer
  Specific Data" et "Service Data" (défaut usine) tous deux gérés, cf.
  _decoder_ela_manufacturer()/_decoder_ela_service() et logique_projet.md.
Chaque paquet BLE est essayé contre les décodeurs de chaque famille tour à
tour ; le reste du pipeline (capteurs.json, MQTT, Kafka, InfluxDB) est
identique quelle que soit la marque du capteur physique.

Ce module réalise quatre fonctions principales :
1. Scanner en permanence les paquets BLE advertising des capteurs reconnus.
2. Décoder les mesures (température, humidité, point de rosée, batterie, RSSI,
   intervalle de log) et les publier sur le broker MQTT cloud (VPS).
3. En cas d'indisponibilité du MQTT cloud (VPS down, service MQTT down, perte
   internet), stocker les mesures dans une base SQLite locale. Dès que la
   connexion est rétablie, les données en attente sont poussées en batch sur le
   MQTT cloud puis supprimées du buffer local.
4. Maintenir à jour le fichier capteurs.json (auto-enregistrement des nouvelles
   MAC, hot-reload des noms et emplacements sans redémarrage, reconfiguration
   GATT périodique des capteurs dont l'intervalle de log n'est pas encore optimisé).

Résilience cloud :
    Lorsque le broker MQTT cloud est inaccessible (VPS down, perte internet,
    service MQTT crashé), les mesures sont accumulées dans ``murmetric_buffer.db``
    (SQLite, même répertoire que ce script). Dès reconnexion, la tâche
    ``tache_sync_sqlite`` pousse les données en lot (batch de SYNC_BATCH_SIZE
    messages) puis les efface de SQLite après confirmation.

Usage :
    python -u ingestion_capteurs_bluetooth.py

    Variables d'environnement reconnues :
        MQTT_BROKER       Adresse du broker MQTT cloud (défaut : localhost)
        MQTT_PORT         Port du broker               (défaut : 1883)
        MQTT_USERNAME     Utilisateur MQTT             (défaut : vide, pas d'auth tentée)
        MQTT_PASSWORD     Mot de passe MQTT            (défaut : vide)
        MQTT_TLS_ENABLED  Active TLS (1/true)          (défaut : désactivé)
        MQTT_CA_CERT      Certificat à faire confiance  (obligatoire si MQTT_TLS_ENABLED)
        MQTT_TOPIC        Topic de publication         (défaut : frd/capteurs/bruts)
        RECONF_INTERVAL   Intervalle reconfiguration en secondes (défaut : 120)
        SQLITE_RETENTION  Rétention du buffer local en jours     (défaut : 7)
        SYNC_BATCH_SIZE   Messages envoyés par batch de rattrapage (défaut : 50)
        SYNC_INTERVAL     Intervalle entre deux tentatives de sync en secondes (défaut : 30)
"""

import asyncio
import json
import math
import os
import re
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
import requests
from bleak import BleakScanner

# Requis pour afficher les accents sous Windows (console cp1252 par défaut).
sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Configuration MQTT — surchargeable via variables d'environnement.
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
# Authentification MQTT (03/08/2026, cf. logique_projet.md) — Mosquitto
# n'accepte plus les connexions anonymes. Vide = pas d'authentification
# tentée.
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
# TLS (04/08/2026, cf. logique_projet.md) — certificat auto-signé (pas de nom
# de domaine sur le VPS) : MQTT_CA_CERT doit pointer vers ce même certificat,
# qui sert alors de racine de confiance unique côté client.
MQTT_TLS_ENABLED = os.getenv("MQTT_TLS_ENABLED", "").lower() in ("1", "true", "yes")
MQTT_CA_CERT = os.getenv("MQTT_CA_CERT", "")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "frd/capteurs/bruts")

# Topic dédié aux métadonnées de configuration des capteurs (registre).
# Publié au démarrage et à chaque modification de capteurs.json.
MQTT_TOPIC_REGISTRE = os.getenv("MQTT_TOPIC_REGISTRE", "frd/capteurs/registre")

# Battement de vie pour le monitoring des pipelines côté webapp (section 32,
# 13/08/2026) — cf. logique_projet.md, monitoring_mqtt.py côté backend.
MQTT_TOPIC_HEARTBEAT = os.getenv("MQTT_TOPIC_HEARTBEAT", "frd/monitoring/heartbeat")
HEARTBEAT_INTERVAL_S = float(os.getenv("HEARTBEAT_INTERVAL_S", "300"))

# ---------------------------------------------------------------------------
# Configuration du buffer SQLite local (résilience cloud).
# ---------------------------------------------------------------------------

# Chemin du fichier SQLite de buffer (même répertoire que ce script).
SQLITE_BUFFER_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "murmetric_buffer.db"
)

# Durée maximale de rétention des mesures en attente (protection contre une
# croissance illimitée du buffer en cas de longue indisponibilité cloud).
SQLITE_RETENTION_JOURS = int(os.getenv("SQLITE_RETENTION", "7"))

# Nombre de messages envoyés par batch lors du rattrapage post-reconnexion.
# Évite de saturer le broker avec des milliers de messages simultanés.
SYNC_BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "50"))

# Intervalle entre deux tentatives de synchronisation du buffer (secondes).
SYNC_INTERVAL_SECONDES = int(os.getenv("SYNC_INTERVAL", "30"))

# ---------------------------------------------------------------------------
# État de la connexion MQTT — partagé entre le thread paho et asyncio.
# ---------------------------------------------------------------------------

# Indique si le client MQTT est actuellement connecté au broker cloud.
# Mis à jour depuis les callbacks paho (thread séparé) — accès thread-safe
# car la lecture/écriture d'un bool est atomique en Python (GIL).
_mqtt_connecte: bool = False

# Horodatage de démarrage du script — publié dans le battement de vie
# (cf. tache_heartbeat) pour que la page Monitoring de la webapp puisse
# afficher depuis quand le process tourne sans interruption.
_DEMARRAGE = datetime.now()

# Compteurs cumulés depuis le démarrage du process (jamais remis à zéro) —
# publiés dans le battement de vie (section 32, 14/08/2026) pour suivre le
# débit d'ingestion dans le temps, pas seulement sa fraîcheur instantanée :
# un débit qui chute sans s'arrêter complètement resterait "OK" côté
# fraîcheur mais se verrait ici. Symétrique de _nb_publies/_nb_bufferises
# déjà présents dans ingestion_dewesoft_dxd.py.
_nb_publies: int = 0
_nb_bufferises: int = 0

# Référence à l'event loop asyncio, stockée au démarrage pour permettre aux
# callbacks paho (thread paho) de signaler des événements à asyncio.
_loop: asyncio.AbstractEventLoop | None = None

# Event asyncio déclenché par on_connect pour réveiller immédiatement la
# tâche de sync au lieu d'attendre le prochain tick de SYNC_INTERVAL.
_sync_event: asyncio.Event | None = None

# ---------------------------------------------------------------------------
# Constantes protocole BLE Blue Maestro.
# ---------------------------------------------------------------------------

# Company ID Bluetooth SIG de Blue Maestro Limited (octets 0x33, 0x01 dans
# l'entête manufacturer data). Bleak le retire du payload et l'expose comme clé
# du dict manufacturer_data — filtre automatique sans liste de MAC.
BLUEMAESTRO_COMPANY_ID = 0x0133

# Adaptateur BLE à utiliser en priorité (Linux/BlueZ uniquement — ignoré sur
# Windows où bleak sélectionne l'unique radio disponible sans configuration).
# Défaut "hci1" : sur le Raspberry Pi de déploiement, hci0 = Bluetooth intégré,
# hci1 = antenne USB externe (portée nettement supérieure, mesurée). Repli
# automatique sur l'adaptateur par défaut si hci1 est indisponible — cf.
# demarrer_scanner_avec_repli(). Surchargeable via variable d'environnement
# si la numérotation diffère sur une autre machine.
BLE_ADAPTER = os.getenv("BLE_ADAPTER", "hci1")

# Versions du protocole Blue Maestro supportées par ce script.
VERSIONS_CONNUES = {13, 23, 27, 41, 42, 43}

# Seuil RSSI en dessous duquel on considère la détection comme un artefact du
# cache BLE Windows (valeur sentinelle -127 dBm, émise ~60 s après le dernier
# vrai paquet, avec octets identiques — pas un vrai paquet radio).
RSSI_MIN_VALIDE = -100

# ---------------------------------------------------------------------------
# Constantes protocole BLE ELA Innovation (Blue Puck RHT — 09/08/2026).
#
# Source : ELA Innovation, "BLE Frame specifications" v12B (elainnovation.com),
# section 6.e "RHT format". Deux modes d'annonce possibles selon la
# configuration NFC du capteur (Blue Puck RHT, réf. IDF25242) :
#   - "Service Data" (mode usine par défaut, aucune configuration requise) :
#     deux blocs Service Data distincts, sur les UUID caractéristiques
#     Bluetooth SIG standard température (0x2A6E) et humidité (0x2A6F).
#   - "Manufacturer Specific Data" (à activer explicitement via l'outil NFC
#     ELA — "Mfr. Data Enable" = True) : company ID ELA suivi d'un octet
#     RHT_DATA_ID, de l'humidité, d'un octet TEMP_DATA_ID, puis de la
#     température.
# Les deux sont gérés ici sans supposer laquelle est active sur un capteur
# donné — aucune configuration NFC préalable n'est nécessaire pour ingérer.
# ---------------------------------------------------------------------------
ELA_COMPANY_ID = 0x0757
ELA_RHT_DATA_ID = 0x21
ELA_TEMP_DATA_ID = 0x12
# UUID complets 128 bits tels qu'exposés par bleak dans advertising_data.service_data
# (forme canonique des UUID 16 bits Bluetooth SIG 0x2A6E/0x2A6F).
ELA_UUID_TEMPERATURE = "00002a6e-0000-1000-8000-00805f9b34fb"
ELA_UUID_HUMIDITE = "00002a6f-0000-1000-8000-00805f9b34fb"
# Batterie (19/08/2026, cf. logique_projet.md section 40 addendum) — UUID
# "Battery Level" standard Bluetooth SIG (0x2A19), réutilisé par ELA comme clé
# du bloc Service Data qui porte le pourcentage. Source : ELA Innovation "BLE
# Frame specifications" v11B, section 5 "Battery information" — ce champ n'est
# transmis par le capteur QUE lorsque sa batterie réelle est déjà sous 15%
# (rien avant ce seuil, aucune notion de pourcentage "sain" annoncé) : absent
# de la trame ne veut donc pas dire "batterie inconnue" comme pour Blue
# Maestro, mais "rien à signaler pour l'instant".
ELA_UUID_BATTERIE = "00002a19-0000-1000-8000-00805f9b34fb"

# ---------------------------------------------------------------------------
# Gestion du registre capteurs — API webapp (source unique, section 32,
# 13/08/2026), avec repli sur capteurs.json local si l'API est injoignable.
#
# capteurs.json mélange deux catégories de champs qui ne vivent PAS au même
# endroit désormais :
# - Champs d'identité/étiquetage (nom, nom_mur, nom_couche, position,
#   prestation, categorie R&D, ingestion, emplacement) : la webapp en est la
#   source de vérité, récupérés par requête HTTP.
# - Champs techniques BLE propres à ce Pi (famille_capteur,
#   mac_complete_connue, numero_capteur_hr_t, lint_configure,
#   lint_max_confirme_s, lint_gatt_absent, lint_gatt_non_supporte) : écrits
#   localement par configure_capteurs.py (reconfiguration GATT), sans rapport
#   avec l'étiquetage mur/couche/position — ce script ne les modifie jamais,
#   seulement les relit depuis capteurs.json pour les fusionner sur les
#   champs d'identité venus de la webapp (cf. _fusionner_champs_techniques).
# ---------------------------------------------------------------------------

CAPTEURS_FILE = os.path.join(os.path.dirname(__file__), "capteurs.json")
CAPTEURS_API_URL = (
    os.getenv("CAPTEURS_API_URL", "http://localhost:8090").rstrip("/")
    + "/api/capteurs/hr_t"
)
INGESTION_API_KEY = os.getenv("INGESTION_API_KEY", "")
# Fréquence d'envoi de la télémétrie (dernière détection/RSSI/batterie,
# 19/08/2026) — throttlée par capteur, indépendamment du rythme réel des
# paquets BLE (souvent plusieurs par minute), pour ne pas solliciter l'API
# à chaque détection. Volontairement peu fréquent : sert à anticiper une
# perte de signal ou une batterie faible, pas à suivre en temps réel.
TELEMETRIE_INTERVALLE_S = int(os.getenv("TELEMETRIE_INTERVAL", str(5 * 60)))
CHAMPS_TECHNIQUES_LOCAUX = (
    "famille_capteur",
    "mac_complete_connue",
    "numero_capteur_hr_t",
    "lint_configure",
    "lint_max_confirme_s",
    "lint_gatt_absent",
    "lint_gatt_non_supporte",
)

# Regex de validation du format d'adresse MAC BLE (XX:XX:XX:XX:XX:XX).
MAC_REGEX = re.compile(r"^([0-9A-F]{2}:){5}[0-9A-F]{2}$", re.IGNORECASE)

# Regex des clés provisoires (backfill HR/T, cf. logique_projet.md) : les 4
# premiers octets d'une MAC BLE réelle, sans ':', tant que les 2 derniers
# octets ne sont pas connus (pas capturés par un export BlueMaestro
# historique — seul un scan BLE réel les révèle). Format délibérément
# distinct d'une vraie MAC (pas de ':') pour qu'aucune confusion ne soit
# possible entre "identité confirmée" et "en attente de réconciliation".
MAC_PROVISOIRE_REGEX = re.compile(r"^[0-9A-F]{8}$", re.IGNORECASE)

# Verrou threading protégeant toutes les lectures/écritures sur capteurs.json.
_fichier_lock = threading.Lock()

# Rafraîchissement du registre distant au rythme de
# CAPTEURS_RAFRAICHISSEMENT_S (pas à chaque appel — verifier_et_recharger_
# capteurs() est appelée depuis des callbacks BLE potentiellement fréquents).
CAPTEURS_RAFRAICHISSEMENT_S = float(os.getenv("CAPTEURS_RAFRAICHISSEMENT_S", "60"))
_capteurs_prochain_rafraichissement: float = 0.0

# Dictionnaire en mémoire : MAC (majuscule) → infos capteur (fusion identité
# webapp + champs techniques locaux).
CAPTEURS_CONNUS: dict = {}


# ===========================================================================
# Buffer SQLite local — résilience cloud.
# ===========================================================================


def initialiser_sqlite() -> None:
    """Créer la table de buffer si elle n'existe pas encore.

    Le schéma stocke chaque message MQTT non encore envoyé au cloud, avec
    son horodatage de collecte pour les politiques de rétention.
    """
    with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS buffer_mqtt (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                topic       TEXT    NOT NULL,
                payload     TEXT    NOT NULL,
                horodatage  TEXT    NOT NULL,
                tente_le    TEXT    DEFAULT NULL
            )
        """)
        # Index sur l'horodatage pour accélérer le tri et la purge.
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_horodatage
            ON buffer_mqtt (horodatage)
        """)
        conn.commit()


def stocker_localement(topic: str, payload_json: str) -> None:
    """Persister un message MQTT dans le buffer SQLite local.

    Appelé lorsque le broker cloud est inaccessible. Le message sera
    renvoyé dès que la connexion sera rétablie.

    Args:
        topic:       Topic MQTT de destination.
        payload_json: Payload JSON sérialisé en chaîne.
    """
    horodatage = datetime.now().isoformat()
    with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
        conn.execute(
            "INSERT INTO buffer_mqtt (topic, payload, horodatage) VALUES (?, ?, ?)",
            (topic, payload_json, horodatage),
        )
        conn.commit()


def purger_buffer_expire() -> int:
    """Supprimer les messages plus anciens que SQLITE_RETENTION_JOURS jours.

    Returns:
        Nombre de messages supprimés.
    """
    limite = (datetime.now() - timedelta(days=SQLITE_RETENTION_JOURS)).isoformat()
    with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
        cursor = conn.execute("DELETE FROM buffer_mqtt WHERE horodatage < ?", (limite,))
        conn.commit()
        return cursor.rowcount


def compter_messages_en_attente() -> int:
    """Retourner le nombre de messages SQLite non encore envoyés."""
    with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
        (count,) = conn.execute("SELECT COUNT(*) FROM buffer_mqtt").fetchone()
        return count


# ===========================================================================
# Publication MQTT avec basculement automatique sur SQLite.
# ===========================================================================


def publier_ou_stocker(topic: str, payload_iot: dict) -> bool:
    """Publier un message MQTT ou le stocker localement si cloud indisponible.

    Stratégie :
    - Si le client MQTT est connecté → tentative de publication directe.
    - En cas d'échec ou de déconnexion → écriture dans SQLite.

    Args:
        topic:       Topic MQTT de destination.
        payload_iot: Dictionnaire de la mesure à publier.

    Returns:
        True si publié directement sur MQTT, False si bufferisé localement
        — utilisé par l'appelant pour incrémenter _nb_publies/
        _nb_bufferises (section 32, 14/08/2026).
    """
    payload_json = json.dumps(payload_iot)

    if _mqtt_connecte:
        try:
            result = mqtt_client.publish(topic, payload_json, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return True  # Publication cloud réussie.
            # Code d'erreur MQTT (buffer plein, non connecté…) → buffer local.
            print(
                f"Attention : publication MQTT cloud échouée (rc={result.rc}) "
                "— stockage local SQLite."
            )
        except Exception as exc:
            print(
                f"Attention : erreur publication MQTT : {exc} — stockage local SQLite."
            )

    # Cloud indisponible ou publication échouée : buffer local.
    stocker_localement(topic, payload_json)
    return False


def publier_registre() -> None:
    """Publier les métadonnées de tous les capteurs connus sur MQTT.

    Chaque entrée de ``CAPTEURS_CONNUS`` est publiée sur
    ``MQTT_TOPIC_REGISTRE``, incluant les capteurs avec ``ingestion: false``
    (non détectés dans les données de mesure). Cela permet à l'application
    cliente et au bridge InfluxDB d'avoir une vue complète du parc de capteurs
    déployés, y compris ceux qui n'émettent pas encore de mesures.

    Appelée :
    - À la connexion MQTT initiale (pour synchroniser l'état courant).
    - À chaque rechargement de capteurs.json (hot-reload ou modification).
    """
    nb = len(CAPTEURS_CONNUS)
    if nb == 0:
        return
    for mac, infos in list(CAPTEURS_CONNUS.items()):
        payload_registre = {
            "mac": mac,
            "nom": infos.get("nom", ""),
            "emplacement": infos.get("emplacement", ""),
            "nom_mur": infos.get("nom_mur", ""),
            "nom_couche": infos.get("nom_couche", ""),
            "position": infos.get("position", ""),
            "prestation": infos.get("prestation", ""),
            "categorie R&D": infos.get("categorie R&D", ""),
            "ingestion": infos.get("ingestion", False),
            "lint_configure": infos.get("lint_configure", False),
            "lint_max_confirme_s": infos.get("lint_max_confirme_s"),
        }
        publier_ou_stocker(MQTT_TOPIC_REGISTRE, payload_registre)
    print(f"Registre capteurs publié sur MQTT ({nb} capteur(s)).")


# ===========================================================================
# Tâche de synchronisation asynchrone du buffer SQLite.
# ===========================================================================


async def tache_sync_sqlite() -> None:
    """Pousser les messages SQLite en attente vers le broker MQTT cloud.

    Cette tâche asyncio tourne en permanence en arrière-plan. Elle se
    déclenche dans deux cas :
    1. Immédiatement à la reconnexion MQTT (via ``_sync_event``).
    2. Périodiquement toutes les ``SYNC_INTERVAL_SECONDES`` secondes.

    Lors de chaque cycle de sync :
    a. Les messages expirés sont purgés (rétention > SQLITE_RETENTION_JOURS).
    b. Les messages en attente sont envoyés par batch de SYNC_BATCH_SIZE.
    c. Chaque message est supprimé de SQLite après confirmation de publication.
    """
    global _sync_event

    while True:
        # Attendre soit la reconnexion MQTT, soit le prochain tick périodique.
        try:
            await asyncio.wait_for(_sync_event.wait(), timeout=SYNC_INTERVAL_SECONDES)
            _sync_event.clear()
        except asyncio.TimeoutError:
            pass  # Tick périodique normal.

        if not _mqtt_connecte:
            continue  # Toujours déconnecté, rien à faire.

        # Purge des messages expirés avant envoi.
        supprimes = purger_buffer_expire()
        if supprimes:
            print(f"SQLite : {supprimes} message(s) expirés supprimés.")

        en_attente = compter_messages_en_attente()
        if en_attente == 0:
            continue

        print(
            f"Synchronisation SQLite → MQTT cloud : "
            f"{en_attente} message(s) en attente."
        )

        envoyes = 0
        erreurs = 0

        with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
            # Lecture par batch, dans l'ordre chronologique.
            rows = conn.execute(
                "SELECT id, topic, payload FROM buffer_mqtt "
                "ORDER BY horodatage ASC LIMIT ?",
                (SYNC_BATCH_SIZE,),
            ).fetchall()

        for row_id, topic, payload in rows:
            if not _mqtt_connecte:
                print("Attention : déconnexion pendant la sync — arrêt du batch.")
                break
            try:
                result = mqtt_client.publish(topic, payload, qos=1)
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    erreurs += 1
                    continue
                # Attendre confirmation de publication (QoS 1, max 5 s).
                result.wait_for_publish(timeout=5.0)
                # Suppression uniquement après confirmation.
                with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
                    conn.execute("DELETE FROM buffer_mqtt WHERE id = ?", (row_id,))
                    conn.commit()
                envoyes += 1
                # Pause légère pour ne pas saturer le broker.
                await asyncio.sleep(0.05)
            except Exception as exc:
                print(f"Attention : erreur sync message {row_id} : {exc}")
                erreurs += 1

        restants = compter_messages_en_attente()
        print(
            f"Sync batch terminé : {envoyes} envoyés, "
            f"{erreurs} erreurs, {restants} restants."
        )

        # S'il reste des messages, se re-déclencher immédiatement.
        if restants > 0 and _mqtt_connecte:
            _sync_event.set()


# ===========================================================================
# Callbacks MQTT — connexion / déconnexion.
# ===========================================================================


def on_connect(client, userdata, flags, rc) -> None:
    """Gérer l'établissement de la connexion MQTT cloud.

    Met à jour l'état global et réveille immédiatement la tâche de sync
    SQLite pour rattraper les messages accumulés hors ligne.
    """
    global _mqtt_connecte
    if rc == 0:
        _mqtt_connecte = True
        print(f"Connecté au Broker MQTT cloud ({MQTT_BROKER}:{MQTT_PORT}) !")
        # Publication immédiate du registre capteurs pour synchroniser l'état
        # courant de capteurs.json avec le bridge InfluxDB dès la reconnexion.
        publier_registre()
        # Signal depuis le thread paho vers l'event loop asyncio.
        if _loop is not None and _sync_event is not None:
            _loop.call_soon_threadsafe(_sync_event.set)
    else:
        print(f"Erreur : connexion MQTT cloud refusée (code {rc})")


def on_disconnect(client, userdata, rc) -> None:
    """Gérer la déconnexion du broker MQTT cloud.

    Passe automatiquement en mode buffer SQLite pour les prochaines mesures.
    """
    global _mqtt_connecte
    _mqtt_connecte = False
    if rc != 0:
        print(
            f"Attention : déconnecté du Broker MQTT cloud (rc={rc}) "
            "— basculement sur SQLite local."
        )


# ===========================================================================
# Gestion du registre capteurs (API webapp + fusion capteurs.json local).
# ===========================================================================


def _valider_entrees(donnees: dict) -> dict:
    """Valider chaque entrée d'un dict brut MAC → infos (même règles qu'avant
    ce chantier : clé provisoire acceptée telle quelle, clé MAC bien formée,
    cohérence clé/champ "mac") — appliqué aussi bien à la réponse de l'API
    webapp qu'au fichier local, qui partagent le même format."""
    resultat = {}
    for mac_cle, infos in donnees.items():
        # Les clés commençant par '_' sont des métadonnées (ex. _schema).
        if mac_cle.startswith("_"):
            continue

        mac_cle_upper = mac_cle.upper()

        # Clé provisoire (backfill HR/T, cf. mac_complete_connue) : acceptée
        # telle quelle, jamais recoupée avec une vraie MAC scannée en direct
        # (device.address a toujours le format complet ':'), donc pas de
        # risque qu'un capteur réel soit pris pour un provisoire ou l'inverse.
        if MAC_PROVISOIRE_REGEX.match(mac_cle_upper):
            resultat[mac_cle_upper] = infos
            continue

        if not MAC_REGEX.match(mac_cle_upper):
            print(f"Attention : clé MAC invalide ignorée : '{mac_cle}'")
            continue

        mac_champ = infos.get("mac", "").upper()
        if mac_champ and mac_champ != mac_cle_upper:
            print(
                f"Attention : incohérence : clé '{mac_cle_upper}' ≠ champ mac "
                f"'{mac_champ}' — entrée ignorée."
            )
            continue

        resultat[mac_cle_upper] = infos

    return resultat


# Dernier résultat connu de _recuperer_registre_distant() — exposé dans le
# battement de vie (cf. tache_heartbeat) pour distinguer "l'API a répondu,
# tout va bien" de "on tourne sur le cache local depuis un moment".
_dernier_registre_api_ok: bool | None = None


def _recuperer_registre_distant() -> dict | None:
    """Récupérer les champs d'identité (mur/couche/position/ingestion/...)
    depuis l'API webapp.

    Returns:
        Le mapping MAC → infos validé, ou None si l'API est injoignable, en
        timeout, répond une erreur HTTP ou un JSON invalide.
    """
    global _dernier_registre_api_ok
    try:
        reponse = requests.get(CAPTEURS_API_URL, timeout=10)
        reponse.raise_for_status()
        donnees = reponse.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"Attention : API capteurs (HR/T) injoignable ({exc}).")
        _dernier_registre_api_ok = False
        return None
    _dernier_registre_api_ok = True
    return _valider_entrees(donnees)


def _lire_capteurs_local_brut() -> dict:
    """Lire capteurs.json local tel quel — champs techniques BLE ET dernier
    état d'identité connu (repli hors-ligne)."""
    try:
        # utf-8-sig : tolère un BOM éventuel (ex. fichier édité/sauvé par un
        # outil qui en ajoute un) en plus de l'UTF-8 sans BOM.
        with open(CAPTEURS_FILE, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        print(f"Attention : {CAPTEURS_FILE} malformé ({e}) — traité comme vide.")
        return {}


def _ecrire_capteurs_local(donnees: dict) -> None:
    try:
        with open(CAPTEURS_FILE, "w", encoding="utf-8") as f:
            json.dump(donnees, f, indent=2, ensure_ascii=False)
    except OSError as exc:
        print(f"Attention : écriture de {CAPTEURS_FILE} impossible ({exc}) — ignoré.")


def _fusionner_champs_techniques(identite: dict) -> dict:
    """Superposer sur chaque entrée d'identité (venue de la webapp) les
    champs techniques BLE déjà connus localement (CHAMPS_TECHNIQUES_LOCAUX)
    — propriété de configure_capteurs.py, sans rapport avec l'étiquetage
    mur/couche/position désormais porté par la webapp."""
    locales = _valider_entrees(_lire_capteurs_local_brut())
    fusion = {}
    for mac, infos in identite.items():
        entree = dict(infos)
        locale = locales.get(mac, {})
        for champ in CHAMPS_TECHNIQUES_LOCAUX:
            if champ in locale:
                entree[champ] = locale[champ]
        fusion[mac] = entree
    return fusion


def _rafraichir_capteurs_connus() -> bool:
    """Récupérer le registre distant (webapp), le fusionner avec les champs
    techniques locaux et écrire le résultat dans capteurs.json — qui sert
    ainsi à la fois de cache de repli hors-ligne et de vue à jour pour
    configure_capteurs.py (exécuté séparément, jamais concurremment en
    pratique). Si l'API est injoignable, retombe sur capteurs.json tel quel
    (dernier état connu, potentiellement périmé côté identité).

    Returns:
        True si CAPTEURS_CONNUS a changé, False sinon.
    """
    global CAPTEURS_CONNUS

    identite = _recuperer_registre_distant()
    if identite is not None:
        fusion = _fusionner_champs_techniques(identite)
        if fusion == CAPTEURS_CONNUS:
            return False
        CAPTEURS_CONNUS = fusion
        _ecrire_capteurs_local(fusion)
        return True

    local = _valider_entrees(_lire_capteurs_local_brut())
    if local and local != CAPTEURS_CONNUS:
        print(f"Registre capteurs repris du cache local ({CAPTEURS_FILE}).")
        CAPTEURS_CONNUS = local
        return True
    return False


def charger_capteurs_connus() -> None:
    """Charger le registre capteurs en mémoire au démarrage du script."""
    global _capteurs_prochain_rafraichissement

    with _fichier_lock:
        _rafraichir_capteurs_connus()
        _capteurs_prochain_rafraichissement = (
            time.monotonic() + CAPTEURS_RAFRAICHISSEMENT_S
        )


def verifier_et_recharger_capteurs() -> None:
    """Rafraîchir le registre depuis l'API webapp, au rythme de
    CAPTEURS_RAFRAICHISSEMENT_S — cette fonction est appelée depuis des
    callbacks BLE potentiellement fréquents, pas question d'y faire une
    requête HTTP à chaque appel."""
    global _capteurs_prochain_rafraichissement

    if time.monotonic() < _capteurs_prochain_rafraichissement:
        return

    with _fichier_lock:
        if _rafraichir_capteurs_connus():
            print(
                f"Registre capteurs rechargé ({len(CAPTEURS_CONNUS)} capteurs connus)"
            )
            # Republier le registre pour refléter les changements dans InfluxDB.
            publier_registre()
        _capteurs_prochain_rafraichissement = (
            time.monotonic() + CAPTEURS_RAFRAICHISSEMENT_S
        )


def enregistrer_capteur_si_inconnu(mac: str, famille: str) -> None:
    """Déclarer une nouvelle MAC auprès de l'API webapp avec ingestion:
    false par défaut. Si l'API est injoignable, l'entrée reste seulement en
    mémoire pour cette exécution (retentée au prochain démarrage) — aucune
    mesure n'est perdue, elle reste simplement non publiée tant que le
    capteur n'est pas étiqueté et activé depuis la webapp.

    Args:
        mac:     Adresse MAC BLE (majuscules).
        famille: "bluemaestro" ou "ela" — cf. champ famille_capteur, utilisé
            par tache_reconfiguration_periodique() pour ne tenter la
            reconfiguration GATT (setlog~/lint, spécifique Nordic UART) que
            sur les capteurs qui la supportent réellement. Un capteur ELA
            marqué "ela" est ignoré indéfiniment par cette tâche — sans ce
            marquage, il apparaîtrait à tort comme "non configuré" à chaque
            cycle (lint_configure n'a pas de sens pour ce protocole), ce qui
            déclencherait un scan+pause GATT toutes les INTERVALLE_RECONF_SECONDES
            (6h par défaut) pour rien.
    """
    global CAPTEURS_CONNUS

    if mac in CAPTEURS_CONNUS:
        return

    entree = {
        "mac": mac,
        "famille_capteur": famille,
        "nom": "",
        "emplacement": "",
        "nom_mur": "",
        "nom_couche": "",
        "position": "",
        "prestation": "",
        "categorie R&D": "",
        "ingestion": False,
    }

    with _fichier_lock:
        try:
            reponse = requests.post(
                CAPTEURS_API_URL + "/enregistrer",
                json={"mac": mac, "famille_capteur": famille},
                headers={"X-Ingestion-Key": INGESTION_API_KEY},
                timeout=10,
            )
            reponse.raise_for_status()
            entree = {**entree, **reponse.json()}
        except (requests.RequestException, ValueError) as exc:
            print(
                f"Attention : enregistrement distant de {mac} impossible ({exc}) — "
                "retenté au prochain démarrage."
            )

        CAPTEURS_CONNUS[mac] = entree
        # Trace locale aussi (repli hors-ligne + visible pour
        # configure_capteurs.py) — fusion avec le fichier existant, pas
        # d'écrasement des autres entrées.
        local = _lire_capteurs_local_brut()
        local[mac] = entree
        _ecrire_capteurs_local(local)

    print(
        f"Nouveau capteur enregistré : {mac} ({famille}) "
        "— définissez ingestion: true depuis la page Capteurs de la webapp pour l'activer"
    )


def envoyer_telemetrie(mac: str, rssi: int, batterie: int | None) -> None:
    """Signale à la webapp la dernière détection/RSSI/batterie d'un capteur
    déjà enregistré — throttlé à un envoi par TELEMETRIE_INTERVALLE_S et par
    MAC, indépendamment du flag ingestion (sert justement à surveiller un
    capteur pas encore activé, ou anticiper une perte de signal avant
    qu'elle ne se produise). Best-effort : une erreur réseau n'interrompt
    jamais le scan, retentée simplement au prochain paquet une fois le délai
    de throttling écoulé — comme enregistrer_capteur_si_inconnu()."""
    maintenant = time.monotonic()
    dernier_envoi = dernier_envoi_telemetrie.get(mac)
    if (
        dernier_envoi is not None
        and maintenant - dernier_envoi < TELEMETRIE_INTERVALLE_S
    ):
        return

    try:
        reponse = requests.post(
            f"{CAPTEURS_API_URL}/{mac}/telemetrie",
            json={"rssi": rssi, "batterie": batterie},
            headers={"X-Ingestion-Key": INGESTION_API_KEY},
            timeout=10,
        )
        reponse.raise_for_status()
        dernier_envoi_telemetrie[mac] = maintenant
    except requests.RequestException as exc:
        print(
            f"Attention : envoi télémétrie de {mac} impossible ({exc}) — retenté plus tard."
        )


# ===========================================================================
# Décodage et calculs physiques.
# ===========================================================================


def calculer_point_de_rosee(
    temperature: float | str,
    humidite: float | str,
) -> float | None:
    """Calculer le point de rosée par la formule de Magnus-Tetens.

    Args:
        temperature: Température en °C. Retourne None si non numérique.
        humidite:    Humidité relative en %. Retourne None si ≤ 0.

    Returns:
        Point de rosée en °C arrondi à 2 décimales, ou None.
    """
    if (
        not isinstance(temperature, (int, float))
        or not isinstance(humidite, (int, float))
        or humidite <= 0
    ):
        return None

    a, b = 17.27, 237.7
    gamma = math.log(humidite / 100.0) + (a * temperature) / (b + temperature)
    return round((b * gamma) / (a - gamma), 2)


# ===========================================================================
# État runtime.
# ===========================================================================

# Horodatage (time.monotonic) de la dernière trame BLE reçue par MAC.
dernieres_detections: dict[str, float] = {}

# Horodatage (time.monotonic) du dernier envoi de télémétrie réussi par MAC
# — sert au throttling de envoyer_telemetrie().
dernier_envoi_telemetrie: dict[str, float] = {}

# Anti-doublon avant publication MQTT/InfluxDB (27/08/2026, demande
# utilisateur) — cf. callback() : (temperature, humidite) et horodatage
# (time.monotonic) de la dernière mesure réellement PUBLIÉE par MAC,
# distincts de dernieres_detections ci-dessus (chaque paquet BLE reçu, pas
# chaque mesure publiée).
dernieres_valeurs_publiees: dict[str, tuple] = {}
dernier_envoi_mesure: dict[str, float] = {}

# ---------------------------------------------------------------------------
# Initialisation SQLite et chargement des capteurs.
# ---------------------------------------------------------------------------
initialiser_sqlite()
charger_capteurs_connus()

en_attente_au_demarrage = compter_messages_en_attente()
if en_attente_au_demarrage:
    print(
        f"Buffer SQLite : {en_attente_au_demarrage} message(s) en attente "
        "de synchronisation cloud."
    )

# ---------------------------------------------------------------------------
# Initialisation du client MQTT avec callbacks de connexion.
# ---------------------------------------------------------------------------
mqtt_client = mqtt.Client()
if MQTT_USERNAME:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
if MQTT_TLS_ENABLED:
    mqtt_client.tls_set(ca_certs=MQTT_CA_CERT)
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect

# Reconnexion automatique paho : tente de se reconnecter en arrière-plan
# si la connexion est perdue, avec backoff exponentiel.
mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)

try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()
    # on_connect sera appelé de façon asynchrone → _mqtt_connecte mis à jour là.
    print(f"Tentative de connexion au Broker MQTT cloud ({MQTT_BROKER})...")
except Exception as exc:
    print(
        f"Attention : connexion MQTT cloud impossible au démarrage ({exc}) "
        "— mode SQLite local activé."
    )
    # On ne lève pas SystemExit : le script démarre en mode dégradé et
    # paho tentera de se reconnecter automatiquement via reconnect_delay_set.
    mqtt_client.loop_start()


# ===========================================================================
# Décodeurs de payload par protocole/fabricant.
#
# Chacun retourne None si le paquet ne correspond pas à son protocole (permet
# au callback d'essayer le suivant), sinon un dict {protocole, temperature,
# humidite, batterie, intervalle_log_secondes} — batterie/intervalle_log_secondes
# valent None si le protocole ne les transmet pas dans ce paquet.
# ===========================================================================


def _decoder_bluemaestro(payload_bytes: list) -> dict | None:
    """Décode une trame Blue Maestro (cf. section 5 logique_projet.md).

    Extrait tel quel de l'ancien corps de callback() — seul le regroupement
    en fonction a changé, pas la logique. Ne retourne None que si l'octet
    version n'est pas reconnu ; au-delà, retourne toujours un résultat (avec
    au besoin temperature/humidite valant "Trame tronquée"/"Erreur index"),
    comportement identique à avant.
    """
    version = payload_bytes[0] if payload_bytes else None
    if version not in VERSIONS_CONNUES:
        return None

    intervalle_log_secondes = None
    try:
        if version in (41, 42, 43) and len(payload_bytes) >= 17:
            # Versions modernes (Disc Maxi) : little-endian, résolution 0.01.
            raw_temp = payload_bytes[15] + (payload_bytes[16] << 8)
            if raw_temp > 32767:
                raw_temp -= 65536
            temperature = raw_temp / 100.0

            if version in (42, 43) and len(payload_bytes) >= 19:
                raw_hum = payload_bytes[17] + (payload_bytes[18] << 8)
                humidite = raw_hum / 100.0
            else:
                humidite = None

            if len(payload_bytes) >= 6:
                raw_interval = (
                    payload_bytes[2]
                    + (payload_bytes[3] << 8)
                    + (payload_bytes[4] << 16)
                    + (payload_bytes[5] << 24)
                )
                intervalle_log_secondes = raw_interval / 10.0

        elif version in (13, 23, 27) and len(payload_bytes) >= 8:
            # Versions legacy (Disc Mini) : big-endian, résolution 0.1.
            raw_temp = (payload_bytes[6] << 8) + payload_bytes[7]
            if raw_temp > 32767:
                raw_temp -= 65536
            temperature = raw_temp / 10.0

            if version in (23, 27) and len(payload_bytes) >= 10:
                raw_hum = (payload_bytes[8] << 8) + payload_bytes[9]
                humidite = raw_hum / 10.0
            else:
                humidite = None

            if len(payload_bytes) >= 4:
                intervalle_log_secondes = (payload_bytes[2] << 8) + payload_bytes[3]
        else:
            temperature, humidite = "Trame tronquée", "Trame tronquée"
    except Exception:
        temperature, humidite = "Erreur index", "Erreur index"

    batterie = payload_bytes[1] if len(payload_bytes) >= 2 else None

    return {
        "protocole": f"bluemaestro_v{version}",
        "temperature": temperature,
        "humidite": humidite,
        "batterie": batterie,
        "intervalle_log_secondes": intervalle_log_secondes,
        "bruts": payload_bytes,
    }


def _decoder_ela_manufacturer(payload_bytes: list) -> dict | None:
    """Décode une trame ELA Innovation en mode "Manufacturer Specific Data".

    Structure (après retrait du company ID 0x0757 par bleak, qui l'expose
    comme clé du dict manufacturer_data — même mécanisme que Blue Maestro) :
        [0]    RHT_DATA_ID (0x21) — seul le format RHT (Blue Puck RHT) est
               reconnu ici ; les autres formats ELA (ID, T seul, MAG, MOV...)
               ont un octet[0] différent et sont donc ignorés (return None),
               pas traités par erreur comme un RHT.
        [1]    Humidité relative (%), uint8 direct, pas de facteur d'échelle.
        [2]    TEMP_DATA_ID (0x12) — sous-identifiant du champ température.
        [3:5]  Température, int16 little-endian, ×0,01 °C, signé.

    Source : ELA Innovation "BLE Frame specifications" v12B, section 6.e.
    """
    if len(payload_bytes) < 5 or payload_bytes[0] != ELA_RHT_DATA_ID:
        return None
    if payload_bytes[2] != ELA_TEMP_DATA_ID:
        return None

    humidite = payload_bytes[1]
    raw_temp = payload_bytes[3] + (payload_bytes[4] << 8)
    if raw_temp > 32767:
        raw_temp -= 65536
    temperature = raw_temp / 100.0

    return {
        "protocole": "ela_rht_mfr",
        "temperature": temperature,
        "humidite": humidite,
        # Non implémenté ici (19/08/2026) : en mode Manufacturer Specific
        # Data, la batterie sous 15% arrive comme un bloc séparé portant le
        # même company ID 0x0757 que le bloc RHT — bleak indexe
        # manufacturer_data par company ID, donc les deux risqueraient de se
        # marcher dessus dans le même dict (une seule valeur par clé). Non
        # testable sans capteur réellement configuré dans ce mode (aucun ici,
        # cf. section 40 addendum) — laissé de côté plutôt que codé à
        # l'aveugle. Implémenté à la place pour le mode "Service Data" (mode
        # usine, cf. _decoder_ela_service), qui est celui réellement utilisé
        # par les capteurs de ce projet.
        "batterie": None,
        "intervalle_log_secondes": None,
        "bruts": payload_bytes,
    }


def _decoder_ela_service(service_data: dict) -> dict | None:
    """Décode une trame ELA Innovation en mode "Service Data" — le mode par
    défaut usine du Blue Puck RHT, sans configuration NFC préalable requise.

    Trois blocs Service Data distincts, sur les UUID caractéristiques
    Bluetooth SIG standard :
        0x2A6E (température) : int16 little-endian, ×0,01 °C, signé.
        0x2A6F (humidité)    : uint8 (%), direct.
        0x2A19 (batterie)    : uint8 (%), direct — présent UNIQUEMENT quand
            la batterie réelle du capteur est déjà sous 15% (cf. doc ELA,
            section 5 "Battery information", 19/08/2026) ; absent sinon, ce
            qui n'a donc pas la même signification que `batterie is None`
            côté Blue Maestro (où l'absence veut dire "jamais reçu").

    Args:
        service_data: advertising_data.service_data (dict UUID -> bytes).

    Source : ELA Innovation "BLE Frame specifications" v12B, section 6.e
    (RHT) et v11B, section 5 (batterie).
    """
    temp_bytes = service_data.get(ELA_UUID_TEMPERATURE)
    hum_bytes = service_data.get(ELA_UUID_HUMIDITE)
    batt_bytes = service_data.get(ELA_UUID_BATTERIE)
    if temp_bytes is None:
        return None  # Sans temperature, rien d'exploitable a publier.

    if len(temp_bytes) < 2:
        return None
    raw_temp = temp_bytes[0] + (temp_bytes[1] << 8)
    if raw_temp > 32767:
        raw_temp -= 65536
    temperature = raw_temp / 100.0

    humidite = hum_bytes[0] if hum_bytes else None
    batterie = batt_bytes[0] if batt_bytes else None

    bruts = list(temp_bytes) + (list(hum_bytes) if hum_bytes else [])
    if batt_bytes:
        bruts += list(batt_bytes)

    return {
        "protocole": "ela_rht_service",
        "temperature": temperature,
        "humidite": humidite,
        "batterie": batterie,
        "intervalle_log_secondes": None,
        "bruts": bruts,
    }


# ===========================================================================
# Callback BLE — cœur de l'ingestion.
# ===========================================================================


def callback(device, advertising_data) -> None:
    """Traiter chaque paquet BLE advertising reçu par le scanner.

    Filtre, décode et publie sur MQTT cloud (ou SQLite si cloud indisponible).

    Args:
        device:           Objet bleak BLEDevice.
        advertising_data: Données advertising (manufacturer_data, RSSI, etc.).
    """
    verifier_et_recharger_capteurs()

    mac_adresse = device.address.upper()
    rssi = advertising_data.rssi
    raw_payload = advertising_data.manufacturer_data

    # Filtres 1+2 — payload reconnu par un décodeur (Blue Maestro ou ELA,
    # 09/08/2026). Le premier company ID/format qui correspond gagne ; les
    # décodeurs suivants ne sont même pas appelés. Un paquet BLE totalement
    # étranger (téléphone, autre objet) ne correspond à aucun des trois et
    # est ignoré silencieusement ici, comme avant pour Blue Maestro seul.
    resultat = None
    if BLUEMAESTRO_COMPANY_ID in raw_payload:
        resultat = _decoder_bluemaestro(list(raw_payload[BLUEMAESTRO_COMPANY_ID]))
    if resultat is None and ELA_COMPANY_ID in raw_payload:
        resultat = _decoder_ela_manufacturer(list(raw_payload[ELA_COMPANY_ID]))
    if resultat is None:
        resultat = _decoder_ela_service(advertising_data.service_data)
    if resultat is None:
        return

    # Filtre 3 — RSSI sentinelle (artefact cache Windows ~60 s).
    if rssi is None or rssi <= RSSI_MIN_VALIDE:
        return

    famille = (
        "bluemaestro" if resultat["protocole"].startswith("bluemaestro_") else "ela"
    )
    enregistrer_capteur_si_inconnu(mac_adresse, famille)
    # Indépendant du flag ingestion (filtre 4 ci-dessous) : sert justement à
    # surveiller un capteur pas encore activé.
    envoyer_telemetrie(mac_adresse, rssi, resultat["batterie"])

    local_name = advertising_data.local_name
    infos_capteur = CAPTEURS_CONNUS.get(mac_adresse, {})
    capteur_id = infos_capteur.get("nom") or local_name or f"Inconnu_{mac_adresse}"
    emplacement = infos_capteur.get("emplacement") or "Emplacement inconnu"

    # Filtre 4 — ingestion désactivée (capteur hors-projet ou non validé).
    if not infos_capteur.get("ingestion", False):
        horodatage_bref = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(
            f"[{horodatage_bref}] {capteur_id} ({mac_adresse}) "
            "détecté — non ingéré (ingestion: false dans capteurs.json)"
        )
        return

    maintenant = time.monotonic()
    derniere_fois = dernieres_detections.get(mac_adresse)
    dernieres_detections[mac_adresse] = maintenant
    intervalle = (
        f"{maintenant - derniere_fois:.2f}s"
        if derniere_fois is not None
        else "premier paquet"
    )
    horodatage = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    print(
        f"[{horodatage}] [{capteur_id} / {emplacement}] "
        f"MAC: {mac_adresse} ({resultat['protocole']}) "
        f"(RSSI: {rssi} dBm, intervalle : {intervalle})"
    )

    # Décodage déjà effectué par le décodeur qui a reconnu ce paquet
    # (Blue Maestro ou ELA, cf. filtres 1+2 plus haut).
    temperature = resultat["temperature"]
    humidite = resultat["humidite"]
    batterie = resultat["batterie"]
    intervalle_log_secondes = resultat["intervalle_log_secondes"]
    point_de_rosee = calculer_point_de_rosee(temperature, humidite)

    payload_iot = {
        "capteur_id": capteur_id,
        "emplacement": emplacement,
        "mac": mac_adresse,
        # Mur/couche/position/catégorie R&D (07/08/2026, cf. backfill HR/T
        # dans logique_projet.md) : mêmes champs que capteurs_retrait.json,
        # ajoutés ici pour que mesures_capteurs porte directement ces tags
        # (comme mesures_dewesoft) sans jointure sur registre_capteurs.
        "nom_mur": infos_capteur.get("nom_mur", ""),
        "nom_couche": infos_capteur.get("nom_couche", ""),
        "position": infos_capteur.get("position", ""),
        "categorie R&D": infos_capteur.get("categorie R&D", ""),
        "horodatage": horodatage,
        "temperature_c": temperature,
        "humidite_percent": humidite,
        "point_de_rosee_c": point_de_rosee,
        "batterie_percent": batterie,
        "rssi_dbm": rssi,
        "intervalle_log_secondes": intervalle_log_secondes,
        "liste_chiffres": resultat["bruts"],
    }

    # Détail temperature/humidite/batterie/octets bruts volontairement pas
    # loggué ligne par ligne (retiré le 26/08/2026) : avec 18 capteurs
    # ingestion:true diffusant en continu, ce bloc à 11 lignes/paquet
    # saturait le journal journald en ~1h (78,8 Mo), rendant tout historique
    # au-delà impossible (cf. logique_projet.md). La ligne concise ci-dessus
    # (id/emplacement/MAC/RSSI/intervalle) suffit au diagnostic courant ;
    # les valeurs décodées restent consultables via la webapp (colonne
    # "Dernière mesure") ou dans payload_iot ci-dessus si besoin ponctuel.

    # Anti-doublon avant publication (27/08/2026, demande utilisateur) : le
    # capteur ne mesure réellement qu'une fois par lint_cible_s (jusqu'à 24h
    # par défaut côté Blue Maestro), mais rediffuse la même valeur en boucle
    # par BLE toutes les quelques secondes — sans ce filtre, InfluxDB
    # recevrait jusqu'à des milliers de points identiques par vraie mesure.
    # Deux filtres combinés (cf. logique_projet.md pour la discussion) :
    # - Fréquence, Blue Maestro uniquement (lint_cible_s n'a de sens que
    #   pour cette famille, cf. reconciliation faite plus haut dans la
    #   session) : au plus une publication par intervalle configuré.
    # - Valeur, toutes familles : ne jamais republier une mesure identique
    #   à la précédente pour ce capteur — filet de sécurité pendant la
    #   fenêtre de transition après un changement de réglage (jusqu'à 6h
    #   avant confirmation GATT), et seul filtre actif pour ELA (pas de
    #   notion de lint_cible_s connue côté Pi pour cette famille).
    if famille == "bluemaestro":
        intervalle_min_s = infos_capteur.get("lint_cible_s") or 86400
        dernier_envoi = dernier_envoi_mesure.get(mac_adresse)
        if dernier_envoi is not None and maintenant - dernier_envoi < intervalle_min_s:
            return

    if dernieres_valeurs_publiees.get(mac_adresse) == (temperature, humidite):
        return
    dernieres_valeurs_publiees[mac_adresse] = (temperature, humidite)
    dernier_envoi_mesure[mac_adresse] = maintenant

    # Publication cloud ou stockage local selon disponibilité.
    global _nb_publies, _nb_bufferises
    if publier_ou_stocker(MQTT_TOPIC, payload_iot):
        _nb_publies += 1
    else:
        _nb_bufferises += 1


# ===========================================================================
# Battement de vie — monitoring des pipelines côté webapp (section 32,
# 13/08/2026). Publié sur le même canal MQTT que les mesures (buffer SQLite
# en secours si le cloud est injoignable) : un battement perdu ou en retard
# n'a aucune conséquence sur les données réelles, contrairement à un point
# de mesure.
# ===========================================================================


async def tache_heartbeat() -> None:
    """Tâche asyncio de fond : publie un battement de vie toutes les
    HEARTBEAT_INTERVAL_S secondes."""
    while True:
        payload = {
            "pipeline": "hr_t",
            "machine": socket.gethostname(),
            "demarre_le": _DEMARRAGE.isoformat(),
            "mqtt_connecte": _mqtt_connecte,
            "buffer_sqlite_en_attente": compter_messages_en_attente(),
            "registre_api_ok": _dernier_registre_api_ok,
            "nb_capteurs_connus": len(CAPTEURS_CONNUS),
            "nb_points_publies": _nb_publies,
            "nb_points_bufferises": _nb_bufferises,
        }
        publier_ou_stocker(MQTT_TOPIC_HEARTBEAT, payload)
        await asyncio.sleep(HEARTBEAT_INTERVAL_S)


# ===========================================================================
# Reconfiguration périodique des capteurs.
# ===========================================================================

# Réduit de 6h à 120s le 27/08/2026 (demande utilisateur, suite au réglage
# lint_cible_s par capteur depuis la webapp) : la vérification elle-même
# (lecture capteurs.json + comparaison lint_max_confirme_s/lint_cible_s)
# ne coûte rien et ne touche jamais le scanner — seul un passage GATT réel
# (si non_configures non vide) le met en pause, donc raccourcir l'intervalle
# de vérification n'a aucun coût tant qu'il n'y a rien à faire. 120s (pas
# 60s) pour garder une marge sur CAPTEURS_RAFRAICHISSEMENT_S (60s,
# fraîcheur du registre local) — inutile de vérifier plus vite que le
# registre lui-même ne se met à jour.
INTERVALLE_RECONF_SECONDES = int(os.getenv("RECONF_INTERVAL", "120"))


async def tache_reconfiguration_periodique(scanner: BleakScanner) -> None:
    """Vérifier et reconfigurer périodiquement les capteurs non optimisés.

    Toutes les ``INTERVALLE_RECONF_SECONDES`` secondes, relit capteurs.json
    et tente de configurer les capteurs dont ``lint_configure`` est absent.
    Le scanner d'ingestion est mis en pause le temps de la configuration.

    Args:
        scanner: L'instance BleakScanner active à mettre en pause.
    """
    await asyncio.sleep(INTERVALLE_RECONF_SECONDES)

    while True:
        try:
            with open(CAPTEURS_FILE, "r", encoding="utf-8-sig") as f:
                donnees = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            donnees = {}

        # famille_capteur absent (entrées créées avant cette distinction,
        # 11/08/2026) => considéré "bluemaestro" par défaut, comportement
        # historique inchangé. Un "ela" explicite est en revanche exclu :
        # lint_configure n'a pas de sens pour ce protocole (configuration
        # par NFC, pas de commande GATT setlog~/lint) — sans cette exclusion,
        # un capteur ELA apparaîtrait "non configuré" indéfiniment et
        # déclencherait un scan+pause GATT à chaque cycle pour rien.
        #
        # Comparaison à lint_cible_s plutôt qu'au seul flag lint_configure
        # (26/08/2026) : un changement de cible depuis la webapp doit
        # redéclencher une reconfiguration même sur un capteur déjà marqué
        # configuré à l'ancienne valeur — même repli 86400 que
        # configure_capteurs.py (LINT_CIBLE_DEFAUT, pas d'import croisé entre
        # les deux scripts). lint_gatt_absent/lint_gatt_non_supporte exclus
        # ici aussi : un capteur définitivement non configurable ne doit pas
        # redéclencher un scan+pause GATT à chaque cycle pour rien non plus.
        non_configures = [
            m
            for m, i in donnees.items()
            if not m.startswith("_")
            and i.get("famille_capteur", "bluemaestro") == "bluemaestro"
            and not i.get("lint_gatt_absent")
            and not i.get("lint_gatt_non_supporte")
            and i.get("lint_max_confirme_s") != (i.get("lint_cible_s") or 86400)
        ]

        if non_configures:
            print(
                f"\nReconfiguration périodique — "
                f"{len(non_configures)} capteur(s) en attente : "
                f"{', '.join(non_configures)}"
            )
            print("    Pause du scan d'ingestion pendant la configuration...")
            await scanner.stop()
            try:
                script = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "configure_capteurs.py",
                )
                proc = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-u",
                    script,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                stdout, _ = await proc.communicate()
                if stdout:
                    print(stdout.decode("utf-8", errors="ignore").strip())
                verifier_et_recharger_capteurs()
            except Exception as exc:
                print(f"    Erreur reconfiguration : {exc}")
            finally:
                await scanner.start()
                print("    Reprise du scan d'ingestion.")
        else:
            print(
                f"Reconfiguration périodique : tous les capteurs configurés. "
                f"Prochain check dans {INTERVALLE_RECONF_SECONDES}s."
            )

        await asyncio.sleep(INTERVALLE_RECONF_SECONDES)


# ===========================================================================
# Point d'entrée asyncio.
# ===========================================================================


async def demarrer_scanner_avec_repli(detection_callback) -> BleakScanner:
    """Démarrer le scanner BLE sur BLE_ADAPTER, avec repli automatique.

    Si l'adaptateur demandé (ex. "hci1", l'antenne USB externe) est
    indisponible — non branchée, RF-kill, pilote non chargé — bleak lève une
    exception au démarrage plutôt qu'au moment de la construction de l'objet.
    Plutôt que de laisser le script planter (et rester silencieux tant que
    personne n'a remarqué que le Pi n'ingère plus rien), on retente sans
    adaptateur précisé : bleak choisit alors le premier disponible (en
    pratique le Bluetooth intégré du Raspberry Pi). Sans effet sur Windows
    (le kwarg "bluez" y est ignoré par le backend WinRT).

    Args:
        detection_callback: Callback appelé pour chaque paquet BLE reçu.

    Returns:
        Le scanner démarré (sur BLE_ADAPTER si possible, en repli sinon).
    """
    if BLE_ADAPTER:
        try:
            scanner = BleakScanner(
                detection_callback=detection_callback,
                bluez={"adapter": BLE_ADAPTER},
            )
            await scanner.start()
            print(
                f"[MurMetric] Scan multi-capteurs démarré (adaptateur {BLE_ADAPTER})."
            )
            return scanner
        except Exception as exc:
            print(
                f"Attention : adaptateur {BLE_ADAPTER} indisponible ({exc}) — "
                "repli sur l'adaptateur Bluetooth par défaut."
            )

    scanner = BleakScanner(detection_callback=detection_callback)
    await scanner.start()
    print("[MurMetric] Scan multi-capteurs démarré (adaptateur par défaut).")
    return scanner


async def main() -> None:
    """Démarrer le scanner BLE et les tâches de fond."""
    global _loop, _sync_event

    # Stocker la référence à l'event loop pour les callbacks paho (thread séparé).
    _loop = asyncio.get_running_loop()
    _sync_event = asyncio.Event()

    scanner = await demarrer_scanner_avec_repli(callback)

    # Tâche de fond : sync SQLite → MQTT cloud (store-and-forward).
    asyncio.create_task(tache_sync_sqlite())

    # Tâche de fond : reconfiguration périodique des capteurs non optimisés.
    asyncio.create_task(tache_reconfiguration_periodique(scanner))

    # Tâche de fond : battement de vie pour le monitoring des pipelines.
    asyncio.create_task(tache_heartbeat())

    try:
        while True:
            await asyncio.sleep(1)
    finally:
        await scanner.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMurMetric arrêté par l'utilisateur.")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
