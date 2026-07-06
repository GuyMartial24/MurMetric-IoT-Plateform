"""
Ingestion continue des capteurs BLE Blue Maestro — MurMetric / FRD-CODEM.

Ce module réalise quatre fonctions principales :
1. Scanner en permanence les paquets BLE advertising des capteurs Blue Maestro.
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
        MQTT_TOPIC        Topic de publication         (défaut : frd/capteurs/bruts)
        RECONF_INTERVAL   Intervalle reconfiguration en secondes (défaut : 21600)
        SQLITE_RETENTION  Rétention du buffer local en jours     (défaut : 7)
        SYNC_BATCH_SIZE   Messages envoyés par batch de rattrapage (défaut : 50)
        SYNC_INTERVAL     Intervalle entre deux tentatives de sync en secondes (défaut : 30)
"""

import asyncio
import json
import math
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta

from bleak import BleakScanner
import paho.mqtt.client as mqtt

# Requis pour afficher les emojis sous Windows (console cp1252 par défaut).
sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Configuration MQTT — surchargeable via variables d'environnement.
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "frd/capteurs/bruts")

# Topic dédié aux métadonnées de configuration des capteurs (registre).
# Publié au démarrage et à chaque modification de capteurs.json.
MQTT_TOPIC_REGISTRE = os.getenv("MQTT_TOPIC_REGISTRE", "frd/capteurs/registre")

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

# Versions du protocole Blue Maestro supportées par ce script.
VERSIONS_CONNUES = {13, 23, 27, 41, 42, 43}

# Seuil RSSI en dessous duquel on considère la détection comme un artefact du
# cache BLE Windows (valeur sentinelle -127 dBm, émise ~60 s après le dernier
# vrai paquet, avec octets identiques — pas un vrai paquet radio).
RSSI_MIN_VALIDE = -100

# ---------------------------------------------------------------------------
# Gestion du fichier capteurs.json.
# ---------------------------------------------------------------------------

CAPTEURS_FILE = os.path.join(os.path.dirname(__file__), "capteurs.json")

# Regex de validation du format d'adresse MAC BLE (XX:XX:XX:XX:XX:XX).
MAC_REGEX = re.compile(r"^([0-9A-F]{2}:){5}[0-9A-F]{2}$", re.IGNORECASE)

# Verrou threading protégeant toutes les lectures/écritures sur capteurs.json.
_fichier_lock = threading.Lock()

# Timestamp de la dernière lecture de capteurs.json — sert au hot-reload.
_capteurs_mtime: float | None = None

# Dictionnaire en mémoire : MAC (majuscule) → infos capteur.
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
    limite = (
        datetime.now() - timedelta(days=SQLITE_RETENTION_JOURS)
    ).isoformat()
    with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
        cursor = conn.execute(
            "DELETE FROM buffer_mqtt WHERE horodatage < ?", (limite,)
        )
        conn.commit()
        return cursor.rowcount


def compter_messages_en_attente() -> int:
    """Retourner le nombre de messages SQLite non encore envoyés."""
    with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
        (count,) = conn.execute(
            "SELECT COUNT(*) FROM buffer_mqtt"
        ).fetchone()
        return count


# ===========================================================================
# Publication MQTT avec basculement automatique sur SQLite.
# ===========================================================================

def publier_ou_stocker(topic: str, payload_iot: dict) -> None:
    """Publier un message MQTT ou le stocker localement si cloud indisponible.

    Stratégie :
    - Si le client MQTT est connecté → tentative de publication directe.
    - En cas d'échec ou de déconnexion → écriture dans SQLite.

    Args:
        topic:       Topic MQTT de destination.
        payload_iot: Dictionnaire de la mesure à publier.
    """
    payload_json = json.dumps(payload_iot)

    if _mqtt_connecte:
        try:
            result = mqtt_client.publish(topic, payload_json, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return  # Publication cloud réussie.
            # Code d'erreur MQTT (buffer plein, non connecté…) → buffer local.
            print(
                f"⚠️ Publication MQTT cloud échouée (rc={result.rc}) "
                "— stockage local SQLite."
            )
        except Exception as exc:
            print(f"⚠️ Erreur publication MQTT : {exc} — stockage local SQLite.")

    # Cloud indisponible ou publication échouée : buffer local.
    stocker_localement(topic, payload_json)


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
            "latitude": infos.get("latitude"),
            "longitude": infos.get("longitude"),
            "altitude_m": infos.get("altitude_m"),
            "prestation": infos.get("prestation", ""),
            "categorie R&D": infos.get("categorie R&D", ""),
            "ingestion": infos.get("ingestion", False),
            "lint_configure": infos.get("lint_configure", False),
            "lint_max_confirme_s": infos.get("lint_max_confirme_s"),
        }
        publier_ou_stocker(MQTT_TOPIC_REGISTRE, payload_registre)
    print(f"📋 Registre capteurs publié sur MQTT ({nb} capteur(s)).")


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
            await asyncio.wait_for(
                _sync_event.wait(), timeout=SYNC_INTERVAL_SECONDES
            )
            _sync_event.clear()
        except asyncio.TimeoutError:
            pass  # Tick périodique normal.

        if not _mqtt_connecte:
            continue  # Toujours déconnecté, rien à faire.

        # Purge des messages expirés avant envoi.
        supprimes = purger_buffer_expire()
        if supprimes:
            print(f"🗑️  SQLite : {supprimes} message(s) expirés supprimés.")

        en_attente = compter_messages_en_attente()
        if en_attente == 0:
            continue

        print(
            f"📤 Synchronisation SQLite → MQTT cloud : "
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
                print("⚠️  Déconnexion pendant la sync — arrêt du batch.")
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
                    conn.execute(
                        "DELETE FROM buffer_mqtt WHERE id = ?", (row_id,)
                    )
                    conn.commit()
                envoyes += 1
                # Pause légère pour ne pas saturer le broker.
                await asyncio.sleep(0.05)
            except Exception as exc:
                print(f"⚠️  Erreur sync message {row_id} : {exc}")
                erreurs += 1

        restants = compter_messages_en_attente()
        print(
            f"✅ Sync batch terminé : {envoyes} envoyés, "
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
        print(f"✅ Connecté au Broker MQTT cloud ({MQTT_BROKER}:{MQTT_PORT}) !")
        # Publication immédiate du registre capteurs pour synchroniser l'état
        # courant de capteurs.json avec le bridge InfluxDB dès la reconnexion.
        publier_registre()
        # Signal depuis le thread paho vers l'event loop asyncio.
        if _loop is not None and _sync_event is not None:
            _loop.call_soon_threadsafe(_sync_event.set)
    else:
        print(f"❌ Connexion MQTT cloud refusée (code {rc})")


def on_disconnect(client, userdata, rc) -> None:
    """Gérer la déconnexion du broker MQTT cloud.

    Passe automatiquement en mode buffer SQLite pour les prochaines mesures.
    """
    global _mqtt_connecte
    _mqtt_connecte = False
    if rc != 0:
        print(
            f"⚠️  Déconnecté du Broker MQTT cloud (rc={rc}) "
            "— basculement sur SQLite local."
        )


# ===========================================================================
# Gestion de capteurs.json.
# ===========================================================================

def _lire_et_valider_fichier() -> dict | None:
    """Lire capteurs.json et valider chaque entrée.

    Returns:
        dict  : entrées valides si le fichier est lisible.
        {}    : si le fichier est introuvable.
        None  : si le JSON est malformé (conserver l'ancien dict).
    """
    try:
        with open(CAPTEURS_FILE, "r", encoding="utf-8") as f:
            donnees = json.load(f)
    except FileNotFoundError:
        print(
            f"⚠️ {CAPTEURS_FILE} introuvable — "
            "les capteurs seront identifiés par leur MAC seule."
        )
        return {}
    except json.JSONDecodeError as e:
        print(
            f"⚠️ {CAPTEURS_FILE} malformé ({e}) — "
            "conservation des données précédentes en mémoire."
        )
        return None

    resultat = {}
    for mac_cle, infos in donnees.items():
        # Les clés commençant par '_' sont des métadonnées (ex. _schema).
        if mac_cle.startswith("_"):
            continue

        mac_cle_upper = mac_cle.upper()

        if not MAC_REGEX.match(mac_cle_upper):
            print(
                f"⚠️ Clé MAC invalide ignorée : '{mac_cle}' — "
                "corrigez le fichier capteurs.json"
            )
            continue

        mac_champ = infos.get("mac", "").upper()
        if mac_champ and mac_champ != mac_cle_upper:
            print(
                f"⚠️ Incohérence : clé '{mac_cle_upper}' ≠ champ mac "
                f"'{mac_champ}' — entrée ignorée."
            )
            continue

        resultat[mac_cle_upper] = infos

    return resultat


def charger_capteurs_connus() -> None:
    """Charger capteurs.json en mémoire au démarrage du script."""
    global CAPTEURS_CONNUS, _capteurs_mtime

    with _fichier_lock:
        nouveau = _lire_et_valider_fichier()
        if nouveau is not None:
            CAPTEURS_CONNUS = nouveau
        try:
            _capteurs_mtime = os.path.getmtime(CAPTEURS_FILE)
        except OSError:
            _capteurs_mtime = None


def verifier_et_recharger_capteurs() -> None:
    """Recharger capteurs.json à chaud si le fichier a été modifié."""
    global CAPTEURS_CONNUS, _capteurs_mtime

    try:
        mtime_actuel = os.path.getmtime(CAPTEURS_FILE)
    except OSError:
        return

    if mtime_actuel == _capteurs_mtime:
        return

    with _fichier_lock:
        nouveau = _lire_et_valider_fichier()
        if nouveau is not None:
            CAPTEURS_CONNUS = nouveau
            print(
                f"🔄 capteurs.json rechargé à chaud "
                f"({len(CAPTEURS_CONNUS)} capteurs connus)"
            )
            # Republier le registre pour refléter les changements dans InfluxDB.
            publier_registre()
        _capteurs_mtime = mtime_actuel


def enregistrer_capteur_si_inconnu(mac: str) -> None:
    """Ajouter une nouvelle MAC dans capteurs.json avec ingestion: false par défaut."""
    global CAPTEURS_CONNUS, _capteurs_mtime

    if mac in CAPTEURS_CONNUS:
        return

    with _fichier_lock:
        try:
            if os.path.exists(CAPTEURS_FILE):
                with open(CAPTEURS_FILE, "r", encoding="utf-8") as f:
                    donnees = json.load(f)
            else:
                donnees = {}
        except (json.JSONDecodeError, OSError):
            return

        macs_existantes = {k.upper() for k in donnees}
        if mac.upper() in macs_existantes:
            return

        donnees[mac] = {
            "mac": mac,
            "nom": "",
            "emplacement": "",
            "latitude": None,
            "longitude": None,
            "altitude_m": None,
            "prestation": "",
            "categorie R&D": "",
            "ingestion": False,
        }

        with open(CAPTEURS_FILE, "w", encoding="utf-8") as f:
            json.dump(donnees, f, indent=2, ensure_ascii=False)

        _capteurs_mtime = os.path.getmtime(CAPTEURS_FILE)

    CAPTEURS_CONNUS[mac] = {
        "mac": mac,
        "nom": "",
        "emplacement": "",
        "latitude": None,
        "longitude": None,
        "altitude_m": None,
        "prestation": "",
        "categorie R&D": "",
        "ingestion": False,
    }
    print(
        f"📝 Nouveau capteur enregistré : {mac} "
        "— définissez ingestion: true pour activer la publication MQTT"
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

# ---------------------------------------------------------------------------
# Initialisation SQLite et chargement des capteurs.
# ---------------------------------------------------------------------------
initialiser_sqlite()
charger_capteurs_connus()

en_attente_au_demarrage = compter_messages_en_attente()
if en_attente_au_demarrage:
    print(
        f"📦 Buffer SQLite : {en_attente_au_demarrage} message(s) en attente "
        "de synchronisation cloud."
    )

# ---------------------------------------------------------------------------
# Initialisation du client MQTT avec callbacks de connexion.
# ---------------------------------------------------------------------------
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect

# Reconnexion automatique paho : tente de se reconnecter en arrière-plan
# si la connexion est perdue, avec backoff exponentiel.
mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)

try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()
    # on_connect sera appelé de façon asynchrone → _mqtt_connecte mis à jour là.
    print(f"⏳ Tentative de connexion au Broker MQTT cloud ({MQTT_BROKER})...")
except Exception as exc:
    print(
        f"⚠️  Connexion MQTT cloud impossible au démarrage ({exc}) "
        "— mode SQLite local activé."
    )
    # On ne lève pas SystemExit : le script démarre en mode dégradé et
    # paho tentera de se reconnecter automatiquement via reconnect_delay_set.
    mqtt_client.loop_start()


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

    # Filtre 1 — company ID Blue Maestro.
    if BLUEMAESTRO_COMPANY_ID not in raw_payload:
        return

    payload_bytes = list(raw_payload[BLUEMAESTRO_COMPANY_ID])
    version = payload_bytes[0] if payload_bytes else None

    # Filtre 2 — version reconnue.
    if version not in VERSIONS_CONNUES:
        return

    # Filtre 3 — RSSI sentinelle (artefact cache Windows ~60 s).
    if rssi is None or rssi <= RSSI_MIN_VALIDE:
        return

    enregistrer_capteur_si_inconnu(mac_adresse)

    local_name = advertising_data.local_name
    infos_capteur = CAPTEURS_CONNUS.get(mac_adresse, {})
    capteur_id = (
        infos_capteur.get("nom") or local_name or f"Inconnu_{mac_adresse}"
    )
    emplacement = infos_capteur.get("emplacement") or "Emplacement inconnu"

    # Filtre 4 — ingestion désactivée (capteur hors-projet ou non validé).
    if not infos_capteur.get("ingestion", False):
        horodatage_bref = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(
            f"🔕 [{horodatage_bref}] {capteur_id} ({mac_adresse}) "
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
        f"🎯 [{horodatage}] [{capteur_id} / {emplacement}] "
        f"MAC: {mac_adresse} "
        f"(RSSI: {rssi} dBm, intervalle : {intervalle})"
    )

    # ------------------------------------------------------------------
    # Décodage du payload selon la version du protocole.
    # ------------------------------------------------------------------
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
                intervalle_log_secondes = (
                    (payload_bytes[2] << 8) + payload_bytes[3]
                )
        else:
            temperature, humidite = "Trame tronquée", "Trame tronquée"
    except Exception:
        temperature, humidite = "Erreur index", "Erreur index"

    batterie = payload_bytes[1] if len(payload_bytes) >= 2 else None
    point_de_rosee = calculer_point_de_rosee(temperature, humidite)

    payload_iot = {
        "capteur_id": capteur_id,
        "emplacement": emplacement,
        "mac": mac_adresse,
        "horodatage": horodatage,
        "temperature_c": temperature,
        "humidite_percent": humidite,
        "point_de_rosee_c": point_de_rosee,
        "batterie_percent": batterie,
        "rssi_dbm": rssi,
        "intervalle_log_secondes": intervalle_log_secondes,
        "liste_chiffres": payload_bytes,
    }

    print(f"📊 [CAPTEUR DECODE] {payload_iot['capteur_id']} ({payload_iot['mac']})")
    print(f"    🌡️ Température   : {payload_iot['temperature_c']} °C")
    print(f"    💧 Humidité      : {payload_iot['humidite_percent']} %")
    print(f"    🌫️ Point de rosée : {payload_iot['point_de_rosee_c']} °C")
    print(f"    🔋 Batterie      : {payload_iot['batterie_percent']} %")
    print(f"    📶 RSSI          : {payload_iot['rssi_dbm']} dBm")
    print(f"    ⏱️ Intervalle log : {payload_iot['intervalle_log_secondes']} s")
    print(f"    🔢 Liste entiers  : {payload_iot['liste_chiffres']}")

    # Indique la destination réelle (cloud ou buffer local).
    dest = "☁️  MQTT cloud" if _mqtt_connecte else "💾 SQLite local"
    print(f"    📡 Destination   : {dest}")
    print("-" * 50)

    # Publication cloud ou stockage local selon disponibilité.
    publier_ou_stocker(MQTT_TOPIC, payload_iot)


# ===========================================================================
# Reconfiguration périodique des capteurs.
# ===========================================================================

INTERVALLE_RECONF_SECONDES = int(os.getenv("RECONF_INTERVAL", str(6 * 3600)))


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
            with open(CAPTEURS_FILE, "r", encoding="utf-8") as f:
                donnees = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            donnees = {}

        non_configures = [
            m for m, i in donnees.items()
            if not m.startswith("_") and not i.get("lint_configure")
        ]

        if non_configures:
            print(
                f"\n🔧 Reconfiguration périodique — "
                f"{len(non_configures)} capteur(s) en attente : "
                f"{', '.join(non_configures)}"
            )
            print("    ⏸️  Pause du scan d'ingestion pendant la configuration...")
            await scanner.stop()
            try:
                script = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "configure_capteurs.py",
                )
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, "-u", script,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                stdout, _ = await proc.communicate()
                if stdout:
                    print(stdout.decode("utf-8", errors="ignore").strip())
                verifier_et_recharger_capteurs()
            except Exception as exc:
                print(f"    ❌ Erreur reconfiguration : {exc}")
            finally:
                await scanner.start()
                print("    ▶️  Reprise du scan d'ingestion.")
        else:
            heures = INTERVALLE_RECONF_SECONDES // 3600
            print(
                f"🔧 Reconfiguration périodique : tous les capteurs configurés. "
                f"Prochain check dans {heures}h."
            )

        await asyncio.sleep(INTERVALLE_RECONF_SECONDES)


# ===========================================================================
# Point d'entrée asyncio.
# ===========================================================================

async def main() -> None:
    """Démarrer le scanner BLE et les tâches de fond."""
    global _loop, _sync_event

    # Stocker la référence à l'event loop pour les callbacks paho (thread séparé).
    _loop = asyncio.get_running_loop()
    _sync_event = asyncio.Event()

    print(
        "🔍 [MurMetric] Scan multi-capteurs démarré. "
        "Secoue ou approche tes capteurs..."
    )
    scanner = BleakScanner(detection_callback=callback)
    await scanner.start()

    # Tâche de fond : sync SQLite → MQTT cloud (store-and-forward).
    asyncio.create_task(tache_sync_sqlite())

    # Tâche de fond : reconfiguration périodique des capteurs non optimisés.
    asyncio.create_task(tache_reconfiguration_periodique(scanner))

    try:
        while True:
            await asyncio.sleep(1)
    finally:
        await scanner.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 MurMetric arrêté par l'utilisateur.")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

