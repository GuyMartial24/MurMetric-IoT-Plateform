"""
Ingestion DeweSoftX par import de fichiers .dxd — MurMetric / FRD-CODEM.
Seule méthode d'ingestion DeweSoftX retenue (licence "Dewesoft NET"
nécessaire au streaming live jugée trop coûteuse pour l'usage prévu,
décision du 23/07/2026 — la méthode live/COM a été abandonnée et retirée
du dépôt le 04/08/2026, cf. logique_projet.md).

Ce script surveille un dossier et traite chaque fichier .dxd qui y apparaît :
extraction des canaux via la librairie DWDataReader, puis publication des
mesures sur le pipeline MQTT → Kafka → InfluxDB (topic frd/dewesoft/bruts,
mesure InfluxDB mesures_dewesoft — cf. logique_projet.md section 12).

Utilisation prévue : DeweSoftX (ou un opérateur) dépose/exporte un .dxd dans
le dossier surveillé (partage réseau, export manuel, tâche planifiée DeweSoftX...) ;
ce script s'en charge sans dépendre d'une connexion live à DeweSoftX.

Prérequis :
    1. Dossier DWDataReader_v5_0_8/ présent à la racine du repo (SDK officiel
       DeweSoft, vendored — cf. https://dewesoft.com/download/developer-downloads).
       Aucune dépendance pip : lecture en ctypes brut de DWDataReaderLib64.dll.
    2. Le dossier surveillé doit être accessible en lecture/écriture (les
       fichiers traités sont déplacés vers des sous-dossiers).

Horodatage : chaque échantillon est republié avec un horodatage ABSOLU
reconstitué (horodatage_mesure_iso = début de fichier .dxd + temps relatif de
l'échantillon). kafka_consumer_influx.py utilise ce champ pour dater le point
InfluxDB avec l'horodatage réel de la mesure — l'import différé d'un .dxd
ancien ne s'empile donc pas sur la date d'exécution du consumer.

Filtrage anti-vibration (Hampel) : les capteurs de retrait sont sensibles aux
vibrations (choc/passage à proximité), ce qui crée des pics ponctuels dans les
courbes — confirmé sur les fichiers réels de data_retrait/ (pics jusqu'à
~17000x le bruit normal lors d'essais, et plus modestement en fonctionnement
courant). Chaque échantillon est donc republié avec DEUX valeurs :
``valeur`` (brute, jamais modifiée) et ``valeur_filtree`` (après filtre de
Hampel — médiane + MAD glissantes). Les deux sont conservées pour permettre
une visualisation brut/filtré côte à côte côté application (aucune donnée
n'est perdue).

Étiquetage mur/couche/position (capteurs_retrait.json, cf. logique_projet.md
section 19) : chaque canal DeweSoft rencontré est auto-enregistré (entrée
vide, ingestion: false) au premier fichier .dxd qui le contient — aucune
donnée n'est publiée pour un canal tant que l'utilisateur n'a pas complété
son étiquetage et mis ingestion à true dans ce fichier. Un même nom de canal
apparaissant deux fois dans un seul fichier (collision) est signalé en
console ET publié comme point InfluxDB (mesure alertes_ingestion, via le
topic MQTT_TOPIC_ALERTES) plutôt que fusionné silencieusement.

⚠️ HAMPEL_SEUIL_K ci-dessous est un réglage par défaut appliqué une fois à
l'ingestion — il ne correspond PAS encore à un réglage ajustable en direct
depuis l'application (ça demanderait de recalculer le filtre à la demande
côté backend/API à partir de ``valeur`` brute, composant qui n'existe pas
encore dans ce dépôt). En attendant, changer ce seuil impose de retraiter les
fichiers .dxd sources (conservés dans DXD_PROCESSED_FOLDER, jamais supprimés)
avec la nouvelle valeur.

Usage :
    python ingestion_dewesoft_dxd.py

    Variables d'environnement reconnues :
        MQTT_BROKER          Adresse du broker MQTT cloud   (défaut : localhost)
        MQTT_PORT            Port MQTT                      (défaut : 1883)
        MQTT_USERNAME        Utilisateur MQTT               (défaut : vide, pas d'auth tentée)
        MQTT_PASSWORD        Mot de passe MQTT              (défaut : vide)
        MQTT_TLS_ENABLED     Active TLS (1/true)             (défaut : désactivé)
        MQTT_CA_CERT         Certificat à faire confiance     (obligatoire si MQTT_TLS_ENABLED)
        MQTT_TOPIC_DEWESOFT  Topic de publication            (défaut : frd/dewesoft/bruts)
        MQTT_TOPIC_ALERTES   Topic anomalies d'ingestion      (défaut : frd/dewesoft/alertes)
        DXD_WATCH_FOLDER     Dossier surveillé               (obligatoire)
        DXD_PROCESSED_FOLDER Sous-dossier fichiers traités    (défaut : <watch>/traites)
        DXD_ERROR_FOLDER     Sous-dossier fichiers en erreur  (défaut : <watch>/erreurs)
        POLL_INTERVAL_DXD    Secondes entre deux scans        (défaut : 5)
        STABLE_CHECKS        Vérifications stables avant traitement (défaut : 3)
        SQLITE_RETENTION     Rétention du buffer local en jours (défaut : 7)
        SYNC_BATCH_SIZE      Messages envoyés par batch de rattrapage (défaut : 50)
        SYNC_INTERVAL        Intervalle entre deux tentatives de sync en secondes (défaut : 30)
        HAMPEL_FENETRE       Demi-largeur de la fenêtre glissante, en échantillons (défaut : 10)
        HAMPEL_SEUIL_K       Multiplicateur du MAD au-delà duquel un point est aberrant (défaut : 8.0)
"""

import ctypes
import json
import os
import shutil
import sqlite3
import statistics
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# SDK officiel DWDataReader (vendored, ctypes brut — pas de dépendance pip).
# ---------------------------------------------------------------------------
_SDK_PYTHON_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "DWDataReader_v5_0_8", "examples", "Python",
)
sys.path.insert(0, _SDK_PYTHON_DIR)

try:
    from DWDataReaderHeader import (
        DWChannel,
        DWFileInfo,
        DWMeasurementInfo,
        READER_HANDLE,
        check_error,
        decode_bytes,
        load_library,
    )
except ImportError:
    print(
        "❌ SDK DWDataReader introuvable.\n"
        f"   Attendu dans : {_SDK_PYTHON_DIR}\n"
        "   Télécharger 'DWDataReader' depuis "
        "https://dewesoft.com/download/developer-downloads et placer le "
        "dossier DWDataReader_v5_0_8/ à la racine du repo."
    )
    sys.exit(1)

# Époque Delphi (TDateTime) utilisée par DeweSoftX pour start_store_time.
_EPOCH_DELPHI = datetime(1899, 12, 30, tzinfo=timezone.utc)

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
MQTT_TOPIC_DEWESOFT = os.getenv("MQTT_TOPIC_DEWESOFT", "frd/dewesoft/bruts")
MQTT_TOPIC_ALERTES = os.getenv("MQTT_TOPIC_ALERTES", "frd/dewesoft/alertes")

# ---------------------------------------------------------------------------
# Registre d'étiquetage des capteurs de retrait (mur/couche/position),
# cf. logique_projet.md section 19 — mêmes principes que capteurs.json côté
# BLE (ingestion_capteurs_bluetooth.py), adaptés à des canaux DeweSoft
# identifiés par nom (pas de MAC, pas de découverte automatique).
# ---------------------------------------------------------------------------
CAPTEURS_RETRAIT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "capteurs_retrait.json"
)

# ---------------------------------------------------------------------------
# Configuration de la surveillance du dossier .dxd.
# ---------------------------------------------------------------------------
DXD_WATCH_FOLDER = os.getenv("DXD_WATCH_FOLDER", "")
DXD_PROCESSED_FOLDER = os.getenv(
    "DXD_PROCESSED_FOLDER",
    os.path.join(DXD_WATCH_FOLDER, "traites") if DXD_WATCH_FOLDER else "",
)
DXD_ERROR_FOLDER = os.getenv(
    "DXD_ERROR_FOLDER",
    os.path.join(DXD_WATCH_FOLDER, "erreurs") if DXD_WATCH_FOLDER else "",
)
POLL_INTERVAL_DXD = float(os.getenv("POLL_INTERVAL_DXD", "5"))
STABLE_CHECKS = int(os.getenv("STABLE_CHECKS", "3"))

# ---------------------------------------------------------------------------
# Filtre anti-vibration (Hampel) — réglage par défaut à l'ingestion.
# ---------------------------------------------------------------------------
HAMPEL_FENETRE = int(os.getenv("HAMPEL_FENETRE", "10"))
HAMPEL_SEUIL_K = float(os.getenv("HAMPEL_SEUIL_K", "8.0"))

# ---------------------------------------------------------------------------
# Configuration buffer SQLite local (résilience cloud) — même schéma que
# ingestion_capteurs_bluetooth.py.
# ---------------------------------------------------------------------------
SQLITE_BUFFER_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "murmetric_buffer.db"
)

SQLITE_RETENTION_JOURS = int(os.getenv("SQLITE_RETENTION", "7"))
SYNC_BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "50"))
SYNC_INTERVAL_SECONDES = int(os.getenv("SYNC_INTERVAL", "30"))

_mqtt_connecte: bool = False

# Réveille immédiatement tache_sync_sqlite() à la reconnexion MQTT, au lieu
# d'attendre le prochain tick périodique de SYNC_INTERVAL_SECONDES.
_sync_event = threading.Event()


# ===========================================================================
# Buffer SQLite local.
# ===========================================================================

def initialiser_sqlite() -> None:
    """Créer la table de buffer si elle n'existe pas."""
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
        conn.commit()


def stocker_localement(topic: str, payload_json: str) -> None:
    """Persister un message MQTT dans le buffer SQLite local."""
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


def publier_ou_stocker(topic: str, payload: dict) -> None:
    """Publier sur MQTT cloud ou stocker localement si cloud indisponible."""
    payload_json = json.dumps(payload)
    if _mqtt_connecte:
        try:
            result = mqtt_client.publish(topic, payload_json, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return
        except Exception as exc:
            print(f"⚠️  Publication MQTT échouée ({exc}) — stockage SQLite.")
    stocker_localement(topic, payload_json)


# ===========================================================================
# Registre d'étiquetage des capteurs de retrait (capteurs_retrait.json).
#
# Symétrique du registre BLE (capteurs.json / ingestion_capteurs_bluetooth.py)
# mais sans adresse MAC : la clé est le nom de canal DeweSoft (ex. "HA1"),
# fixé une fois pour toutes par le câblage du rig — pas de découverte
# automatique équivalente au BLE, cf. logique_projet.md section 19.
# ===========================================================================

_fichier_retrait_lock = threading.Lock()
_capteurs_retrait_mtime: float | None = None
CAPTEURS_RETRAIT_CONNUS: dict = {}


def _lire_et_valider_fichier_retrait() -> dict | None:
    """Lire capteurs_retrait.json et retourner le mapping canal → infos.

    Contrairement à capteurs.json (BLE), aucun format de clé à valider (pas
    de regex MAC) : un nom de canal DeweSoft est une chaîne libre.

    Returns:
        Le mapping canal → infos, ou {} si le fichier n'existe pas encore,
        ou None si le JSON est malformé (le registre en mémoire est alors
        conservé tel quel plutôt que vidé).
    """
    try:
        with open(CAPTEURS_RETRAIT_FILE, "r", encoding="utf-8") as f:
            donnees = json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        print(f"⚠️  {CAPTEURS_RETRAIT_FILE} invalide (JSON malformé) : {e} — registre inchangé.")
        return None

    return {cle: infos for cle, infos in donnees.items() if not cle.startswith("_")}


def charger_capteurs_retrait_connus() -> None:
    """Charger capteurs_retrait.json en mémoire au démarrage du script."""
    global CAPTEURS_RETRAIT_CONNUS, _capteurs_retrait_mtime
    with _fichier_retrait_lock:
        nouveau = _lire_et_valider_fichier_retrait()
        if nouveau is not None:
            CAPTEURS_RETRAIT_CONNUS = nouveau
        try:
            _capteurs_retrait_mtime = os.path.getmtime(CAPTEURS_RETRAIT_FILE)
        except OSError:
            _capteurs_retrait_mtime = None


def verifier_et_recharger_capteurs_retrait() -> None:
    """Recharger capteurs_retrait.json à chaud si le fichier a été modifié."""
    global CAPTEURS_RETRAIT_CONNUS, _capteurs_retrait_mtime
    try:
        mtime_actuel = os.path.getmtime(CAPTEURS_RETRAIT_FILE)
    except OSError:
        return
    if mtime_actuel == _capteurs_retrait_mtime:
        return
    with _fichier_retrait_lock:
        nouveau = _lire_et_valider_fichier_retrait()
        if nouveau is not None:
            CAPTEURS_RETRAIT_CONNUS = nouveau
            print(f"🔄 capteurs_retrait.json rechargé à chaud ({len(CAPTEURS_RETRAIT_CONNUS)} canal(aux) connus)")
        _capteurs_retrait_mtime = mtime_actuel


def enregistrer_canal_si_inconnu(canal_nom: str) -> None:
    """Ajouter un nouveau canal dans capteurs_retrait.json avec ingestion: false.

    Aucun canal n'est donc ingéré silencieusement : sa première lecture crée
    une entrée vide à étiqueter, exactement comme une MAC BLE inconnue dans
    capteurs.json.
    """
    global CAPTEURS_RETRAIT_CONNUS, _capteurs_retrait_mtime

    if canal_nom in CAPTEURS_RETRAIT_CONNUS:
        return

    with _fichier_retrait_lock:
        try:
            if os.path.exists(CAPTEURS_RETRAIT_FILE):
                with open(CAPTEURS_RETRAIT_FILE, "r", encoding="utf-8") as f:
                    donnees = json.load(f)
            else:
                donnees = {}
        except (json.JSONDecodeError, OSError):
            return

        if canal_nom in donnees:
            return

        entree = {
            "canal": canal_nom,
            "nom_mur": "",
            "nom_couche": "",
            "position": "",
            "categorie R&D": "",
            "prestation": "",
            "ingestion": False,
        }
        donnees[canal_nom] = entree

        with open(CAPTEURS_RETRAIT_FILE, "w", encoding="utf-8") as f:
            json.dump(donnees, f, indent=2, ensure_ascii=False)

        _capteurs_retrait_mtime = os.path.getmtime(CAPTEURS_RETRAIT_FILE)

    CAPTEURS_RETRAIT_CONNUS[canal_nom] = entree
    print(
        f"📝 Nouveau canal de retrait enregistré : {canal_nom} "
        "— définissez ingestion: true dans capteurs_retrait.json pour l'activer"
    )


# ===========================================================================
# Callbacks MQTT.
# ===========================================================================

def on_connect(client, userdata, flags, rc) -> None:
    global _mqtt_connecte
    if rc == 0:
        _mqtt_connecte = True
        print(f"✅ Connecté au Broker MQTT cloud ({MQTT_BROKER}:{MQTT_PORT})")
        # Réveille immédiatement la tâche de sync pour rattraper le buffer.
        _sync_event.set()
    else:
        print(f"❌ Connexion MQTT cloud refusée (code {rc})")


def on_disconnect(client, userdata, rc) -> None:
    global _mqtt_connecte
    _mqtt_connecte = False
    if rc != 0:
        print(f"⚠️  Déconnecté du Broker MQTT cloud (rc={rc}) — basculement SQLite.")


# ===========================================================================
# Tâche de resynchronisation du buffer SQLite → MQTT cloud.
# ===========================================================================

def tache_sync_sqlite() -> None:
    """Pousser les messages SQLite en attente vers le broker MQTT cloud.

    Tourne dans un thread dédié en arrière-plan pendant toute la durée du
    script. Se déclenche dans deux cas :
    1. Immédiatement à la reconnexion MQTT (via ``_sync_event``, réveillé
       par ``on_connect``).
    2. Périodiquement toutes les ``SYNC_INTERVAL_SECONDES`` secondes, en
       secours si aucune reconnexion n'a eu lieu entre-temps.

    À chaque cycle : purge des messages expirés, puis envoi par batch de
    ``SYNC_BATCH_SIZE`` (les plus anciens d'abord), suppression de chaque
    message uniquement après confirmation de publication (QoS 1).
    """
    while True:
        _sync_event.wait(timeout=SYNC_INTERVAL_SECONDES)
        _sync_event.clear()

        if not _mqtt_connecte:
            continue  # Toujours déconnecté, rien à faire.

        supprimes = purger_buffer_expire()
        if supprimes:
            print(f"🗑️  SQLite : {supprimes} message(s) expirés supprimés.")

        en_attente = compter_messages_en_attente()
        if en_attente == 0:
            continue

        print(f"📤 Synchronisation SQLite → MQTT cloud : {en_attente} message(s) en attente.")

        envoyes = 0
        erreurs = 0

        with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
            rows = conn.execute(
                "SELECT id, topic, payload FROM buffer_mqtt ORDER BY horodatage ASC LIMIT ?",
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
                result.wait_for_publish(timeout=5.0)
                with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
                    conn.execute("DELETE FROM buffer_mqtt WHERE id = ?", (row_id,))
                    conn.commit()
                envoyes += 1
                time.sleep(0.05)  # Pause légère pour ne pas saturer le broker.
            except Exception as exc:
                print(f"⚠️  Erreur sync message {row_id} : {exc}")
                erreurs += 1

        restants = compter_messages_en_attente()
        print(f"✅ Sync batch terminé : {envoyes} envoyés, {erreurs} erreurs, {restants} restants.")

        # S'il reste des messages, se re-déclencher immédiatement.
        if restants > 0 and _mqtt_connecte:
            _sync_event.set()


# ===========================================================================
# Détection de fichier stable (copie/écriture terminée).
# ===========================================================================

def fichier_est_verrouille(chemin: str) -> bool:
    """Détecter si un processus tient encore le fichier ouvert en écriture.

    Astuce Windows : un renommage vers son propre nom échoue si un autre
    processus a un descripteur ouvert sur le fichier.
    """
    try:
        os.rename(chemin, chemin)
        return False
    except OSError:
        return True


def attendre_fichier_stable(chemin: str) -> bool:
    """Attendre que la taille du fichier soit stable et qu'il ne soit plus verrouillé.

    Returns:
        True si le fichier est prêt à être traité, False s'il a disparu
        (déplacé/supprimé entre-temps) avant stabilisation.
    """
    taille_precedente = -1
    verifications_stables = 0

    while verifications_stables < STABLE_CHECKS:
        if not os.path.isfile(chemin):
            return False
        try:
            taille = os.path.getsize(chemin)
        except OSError:
            return False

        if taille == taille_precedente and taille > 0 and not fichier_est_verrouille(chemin):
            verifications_stables += 1
        else:
            verifications_stables = 0

        taille_precedente = taille
        time.sleep(POLL_INTERVAL_DXD)

    return True


# ===========================================================================
# Filtre de Hampel (médiane + MAD glissantes) — rejette les pics de vibration.
# ===========================================================================

def filtrer_hampel(
    valeurs: list[float],
    demi_fenetre: int = HAMPEL_FENETRE,
    seuil_k: float = HAMPEL_SEUIL_K,
) -> tuple[list[float], set[int]]:
    """Détecter et remplacer les pics de vibration par un filtre de Hampel.

    Un pic de vibration se traduit par un saut ponctuel d'amplitude
    physiquement impossible pour un phénomène de retrait (qui évolue
    lentement) — confirmé sur les fichiers réels de data_retrait/ (sauts
    jusqu'à ~17000x le bruit normal). Contrairement à une moyenne glissante,
    la médiane locale n'est pas tirée vers le pic : un point aberrant est
    simplement remplacé par la médiane de sa fenêtre.

    Args:
        valeurs: Échantillons bruts d'un canal, à pas d'échantillonnage régulier.
        demi_fenetre: Nombre d'échantillons pris de part et d'autre du point testé.
        seuil_k: Multiplicateur du MAD (écart absolu médian) au-delà duquel un
            point est jugé aberrant. Plus petit = filtre plus strict.

    Returns:
        Tuple (valeurs_filtrees, indices_aberrants).
    """
    n = len(valeurs)
    filtrees = list(valeurs)
    aberrants: set[int] = set()
    # Facteur de conversion MAD → écart-type équivalent pour une distribution
    # normale (convention standard du filtre de Hampel).
    facteur_mad = 1.4826

    for i in range(n):
        lo = max(0, i - demi_fenetre)
        hi = min(n, i + demi_fenetre + 1)
        fenetre = valeurs[lo:hi]
        mediane = statistics.median(fenetre)
        mad = statistics.median([abs(v - mediane) for v in fenetre]) * facteur_mad

        # Plancher : le MAD de la fenêtre peut tomber exactement à zéro quand
        # plusieurs échantillons y sont identiques (quantification du
        # capteur — observé sur les fichiers réels), ce qui désactiverait la
        # détection au lieu de la déclencher. On utilise alors le MAD des
        # DIFFÉRENCES successives dans la même fenêtre comme échelle de repli :
        # coût négligeable (même fenêtre, déjà en mémoire) et insensible à la
        # dérive lente du signal (contrairement à un plancher basé sur tout
        # le fichier, qui se révèle trop large — testé et écarté).
        diffs_fenetre = [fenetre[k] - fenetre[k - 1] for k in range(1, len(fenetre))]
        mad_diff = 0.0
        if diffs_fenetre:
            mediane_diff = statistics.median(diffs_fenetre)
            mad_diff = statistics.median([abs(d - mediane_diff) for d in diffs_fenetre]) * facteur_mad

        mad_effectif = max(mad, mad_diff)
        if mad_effectif > 0 and abs(valeurs[i] - mediane) > seuil_k * mad_effectif:
            aberrants.add(i)
            filtrees[i] = mediane

    return filtrees, aberrants


# ===========================================================================
# Extraction .dxd → publication MQTT.
# ===========================================================================

def extraire_et_publier(chemin: str) -> tuple[int, int]:
    """Extraire tous les canaux d'un fichier .dxd et publier chaque échantillon.

    Utilise la DLL officielle DWDataReaderLib64.dll en ctypes brut (validé
    dans test_lecture_dxd.py sur les fichiers de data_retrait/).

    Args:
        chemin: Chemin absolu vers le fichier .dxd à traiter.

    Returns:
        Tuple (nombre de canaux, nombre total de mesures publiées).
    """
    nom_fichier = os.path.basename(chemin)
    horodatage_lecture = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    nb_mesures = 0

    lib = load_library(os.path.join(_SDK_PYTHON_DIR, "DWDataReaderLib64.dll"))
    reader = READER_HANDLE()
    check_error(lib, lib.DWICreateReader(ctypes.byref(reader)))

    try:
        c_filename = ctypes.c_char_p(chemin.encode())
        file_info = DWFileInfo(0, 0, 0)
        check_error(lib, lib.DWIOpenDataFile(reader, c_filename, ctypes.byref(file_info)))

        try:
            mesure_info = DWMeasurementInfo(0, 0, 0, 0)
            check_error(lib, lib.DWIGetMeasurementInfo(reader, ctypes.byref(mesure_info)))
            horodatage_debut = _EPOCH_DELPHI + timedelta(days=mesure_info.start_store_time)

            ch_count = ctypes.c_int()
            check_error(lib, lib.DWIGetChannelListCount(reader, ctypes.byref(ch_count)))

            channel_list = (DWChannel * ch_count.value)()
            check_error(lib, lib.DWIGetChannelList(reader, channel_list))

            # Garde-fou collision de canal (cf. logique_projet.md section 19) :
            # tous les canaux de retrait (murs confondus) vivent dans le même
            # fichier .dxd, sans séparation structurelle — un même nom de
            # canal utilisé deux fois dans CE fichier ne serait jamais fusionné
            # (chaque canal reste traité séparément via son canal_index), mais
            # l'anomalie doit être signalée car le registre capteurs_retrait.json
            # ne peut alors pas savoir lequel des deux étiqueter correctement.
            noms_canaux = [decode_bytes(channel_list[k].name) for k in range(ch_count.value)]
            compte_noms = Counter(noms_canaux)
            for nom_duplique, occurrences in compte_noms.items():
                if occurrences <= 1:
                    continue
                print(
                    f"🚨 COLLISION DE CANAL : \"{nom_duplique}\" apparaît "
                    f"{occurrences} fois dans {nom_fichier} — étiquetage "
                    "mur/couche/position potentiellement incorrect pour ce canal."
                )
                publier_ou_stocker(MQTT_TOPIC_ALERTES, {
                    "type": "collision_canal",
                    "canal_nom": nom_duplique,
                    "fichier_source": nom_fichier,
                    "occurrences": occurrences,
                    "horodatage": horodatage_lecture,
                })

            for i in range(ch_count.value):
                ch = channel_list[i]
                nom_canal = decode_bytes(ch.name)
                unite = decode_bytes(ch.unit)

                enregistrer_canal_si_inconnu(nom_canal)
                infos_canal = CAPTEURS_RETRAIT_CONNUS.get(nom_canal, {})
                if not infos_canal.get("ingestion", False):
                    print(
                        f"   ⏭️  {nom_canal} : ingestion désactivée "
                        "(capteurs_retrait.json) — canal ignoré."
                    )
                    continue

                sample_cnt = ctypes.c_longlong()
                check_error(
                    lib, lib.DWIGetScaledSamplesCount(reader, ch.index, ctypes.byref(sample_cnt))
                )
                if sample_cnt.value == 0:
                    continue

                samples = (ctypes.c_double * sample_cnt.value)()
                timestamps = (ctypes.c_double * sample_cnt.value)()
                check_error(
                    lib,
                    lib.DWIGetScaledSamples(reader, ch.index, 0, sample_cnt, samples, timestamps),
                )

                taux = None
                if sample_cnt.value > 1 and timestamps[1] != timestamps[0]:
                    taux = 1.0 / (timestamps[1] - timestamps[0])

                # Filtre de Hampel appliqué sur la série complète du canal —
                # a besoin des voisins passés ET futurs, donc calculé ici en
                # une passe plutôt que point par point.
                valeurs_filtrees, indices_aberrants = filtrer_hampel(list(samples))

                for j in range(sample_cnt.value):
                    horodatage_mesure = horodatage_debut + timedelta(seconds=timestamps[j])
                    payload = {
                        "source": "dewesoft",
                        "methode": "import_dxd",
                        "fichier_source": nom_fichier,
                        "canal_index": i,
                        "canal_nom": nom_canal,
                        "canal_unite": unite,
                        "nom_mur": infos_canal.get("nom_mur", ""),
                        "nom_couche": infos_canal.get("nom_couche", ""),
                        "position": infos_canal.get("position", ""),
                        "categorie R&D": infos_canal.get("categorie R&D", ""),
                        "valeur": samples[j],
                        "valeur_filtree": valeurs_filtrees[j],
                        "est_aberrant": j in indices_aberrants,
                        "horodatage_dewesoft": timestamps[j],
                        "horodatage_mesure_iso": horodatage_mesure.isoformat(),
                        "horodatage": horodatage_lecture,
                        "taux_echantillonnage": taux,
                    }
                    publier_ou_stocker(MQTT_TOPIC_DEWESOFT, payload)
                    nb_mesures += 1

                    if nb_mesures % 1000 == 0:
                        print(f"   ... {nb_mesures} mesure(s) publiées")

                if indices_aberrants:
                    print(
                        f"   🧹 {nom_canal} : {len(indices_aberrants)} pic(s) "
                        f"filtré(s) sur {sample_cnt.value} échantillon(s)."
                    )

            return ch_count.value, nb_mesures
        finally:
            check_error(lib, lib.DWICloseDataFile(reader))
    finally:
        check_error(lib, lib.DWIDestroyReader(reader))


# ===========================================================================
# Déplacement des fichiers traités / en erreur.
# ===========================================================================

def deplacer_sans_ecraser(chemin: str, dossier_cible: str) -> None:
    """Déplacer un fichier vers un dossier, en évitant d'écraser un homonyme."""
    os.makedirs(dossier_cible, exist_ok=True)
    nom_fichier = os.path.basename(chemin)
    destination = os.path.join(dossier_cible, nom_fichier)

    if os.path.exists(destination):
        base, ext = os.path.splitext(nom_fichier)
        horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
        destination = os.path.join(dossier_cible, f"{base}_{horodatage}{ext}")

    shutil.move(chemin, destination)


# ===========================================================================
# Boucle de surveillance du dossier.
# ===========================================================================

def boucle_surveillance() -> None:
    """Scanner le dossier surveillé en continu et traiter chaque .dxd détecté."""
    print(f"\n👀 Surveillance du dossier : {DXD_WATCH_FOLDER}")
    print(f"   Poll toutes les {POLL_INTERVAL_DXD} s. Arrêt : Ctrl+C\n")

    deja_signales: set[str] = set()

    while True:
        verifier_et_recharger_capteurs_retrait()

        try:
            noms_fichiers = os.listdir(DXD_WATCH_FOLDER)
        except OSError as exc:
            print(f"❌ Dossier surveillé inaccessible ({exc}) — nouvelle tentative...")
            time.sleep(POLL_INTERVAL_DXD)
            continue

        for nom in noms_fichiers:
            if not nom.lower().endswith(".dxd"):
                continue
            chemin = os.path.join(DXD_WATCH_FOLDER, nom)
            if not os.path.isfile(chemin):
                continue

            if nom not in deja_signales:
                print(f"📄 Nouveau fichier détecté : {nom}")
                deja_signales.add(nom)

            if not attendre_fichier_stable(chemin):
                continue  # disparu entre-temps (déjà traité ailleurs, etc.)

            print(f"⏳ Extraction de {nom}...")
            try:
                nb_canaux, nb_mesures = extraire_et_publier(chemin)
                print(f"✅ {nom} : {nb_canaux} canal(aux), {nb_mesures} mesure(s) publiées.")
                deplacer_sans_ecraser(chemin, DXD_PROCESSED_FOLDER)
            except Exception as exc:
                print(f"❌ Échec extraction {nom} : {exc}")
                deplacer_sans_ecraser(chemin, DXD_ERROR_FOLDER)

            deja_signales.discard(nom)

        time.sleep(POLL_INTERVAL_DXD)


# ===========================================================================
# Point d'entrée.
# ===========================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("  MurMetric — Import DeweSoftX par dépôt de fichiers .dxd (Plan B)")
    print("=" * 60)

    if not DXD_WATCH_FOLDER:
        print("❌ DXD_WATCH_FOLDER non défini. Exemple :")
        print(r'   set DXD_WATCH_FOLDER=C:\Users\Public\Documents\Dewesoft\Data && python ingestion_dewesoft_dxd.py')
        sys.exit(1)

    os.makedirs(DXD_WATCH_FOLDER, exist_ok=True)
    os.makedirs(DXD_PROCESSED_FOLDER, exist_ok=True)
    os.makedirs(DXD_ERROR_FOLDER, exist_ok=True)

    initialiser_sqlite()
    charger_capteurs_retrait_connus()

    mqtt_client = mqtt.Client()
    if MQTT_USERNAME:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS_ENABLED:
        mqtt_client.tls_set(ca_certs=MQTT_CA_CERT)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)

    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
        print(f"⏳ Connexion MQTT cloud ({MQTT_BROKER})...")
    except Exception as exc:
        print(f"⚠️  MQTT cloud indisponible ({exc}) — mode SQLite local activé.")
        mqtt_client.loop_start()

    # --- Tâche de resynchronisation SQLite → MQTT (arrière-plan) ---
    threading.Thread(target=tache_sync_sqlite, daemon=True).start()

    try:
        boucle_surveillance()
    except KeyboardInterrupt:
        print("\n🛑 Arrêt demandé par l'utilisateur.")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("👋 ingestion_dewesoft_dxd.py terminé.")
