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
l'échantillon, RAMENÉ À L'ORIGINE DU FICHIER — les horodatages relatifs du SDK
sont comptés depuis le début de la session d'acquisition, pas du fichier ;
cf. le bloc « Origine des horodatages relatifs » dans extraire_et_publier()). kafka_consumer_influx.py utilise ce champ pour dater le point
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
        MQTT_MAX_INFLIGHT    Messages QoS 1 simultanés en vol (défaut : 1000)
        MQTT_MAX_EN_ATTENTE  Plafond de messages QoS 1 non acquittés (défaut : 20000)
        MQTT_ATTENTE_TIMEOUT Attente max d'une place avant bascule SQLite, en s (défaut : 60)
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
        DXD_BATCH_SIZE       Échantillons regroupés par message MQTT (défaut : 600)
        TOLERANCE_PAS_S      Écart max à la grille t0+k*dt avant de couper un lot (défaut : 1e-6)
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
# Nombre de messages QoS 1 "en vol" (envoyés, en attente d'accusé de réception)
# autorisés simultanément — le défaut paho-mqtt (20) plafonne le débit à
# ~20/aller-retour réseau, ce qui devient le goulot d'étranglement dominant
# sur un import massif de fichiers .dxd historiques (mesuré : ~374 msg/s
# Amiens→VPS avec le défaut). Aucun changement de fiabilité ni de contenu —
# juste plus d'envois simultanés avant d'attendre l'accusé de réception.
MQTT_MAX_INFLIGHT = int(os.getenv("MQTT_MAX_INFLIGHT", "1000"))

# ---------------------------------------------------------------------------
# Contre-pression QoS 1 (06/08/2026, cf. logique_projet.md).
#
# PROBLÈME MESURÉ : mqtt_client.publish(..., qos=1) ne fait qu'empiler le
# message dans la file interne de paho et rend la main immédiatement. La
# boucle d'extraction soumettait donc ~21 000 msg/s alors que le réseau n'en
# évacuait que ~1 100/s. La file interne enflait jusqu'à ce que l'identifiant
# de message paho — un compteur sur 16 BITS, donc 65 535 valeurs — reboucle
# et retombe sur un identifiant encore non acquitté. À partir de là, paho
# renvoie MQTT_ERR_QUEUE_SIZE (rc=15) pour CHAQUE publication.
#
# Or publier_ou_stocker() interprétait tout rc non nul comme « cloud
# indisponible » et basculait le message vers SQLite — alors que la connexion
# MQTT était parfaitement saine. D'où le symptôme observé : le buffer SQLite
# local qui grossit sans fin, avec des horodatages « maintenant », pendant que
# le broker est joignable. Mesuré sur 200 000 messages sans contre-pression :
# premier échec à i=70 186 avec exactement 65 535 messages en attente, puis
# 127 320 messages sur 200 000 (64 %) refusés et détournés vers SQLite.
#
# CORRECTIF : borner le nombre de messages QoS 1 non acquittés bien en-dessous
# de 65 535. La boucle d'extraction se cale alors sur le débit réel du
# pipeline au lieu de le devancer, publish() ne rate plus jamais, et SQLite
# redevient ce qu'il doit être : un secours en cas de vraie coupure réseau.
# ---------------------------------------------------------------------------
# RECALIBRÉ le 07/08/2026 pour la publication par lots : ce plafond compte des
# MESSAGES, pas des échantillons. Tant qu'un message = un échantillon, 20 000
# messages en vol = 20 000 échantillons. Depuis le passage aux lots de 600, la
# même valeur autoriserait 12 MILLIONS d'échantillons en vol — soit plus que le
# travail total d'un fichier : la contre-pression ne s'exerçait donc plus du
# tout, l'extraction rendait la main instantanément et le fichier était déclaré
# traité alors que la quasi-totalité des messages dormait encore dans la file
# paho. 200 messages ≈ 120 000 échantillons ≈ 3,4 Mo en vol : assez pour
# saturer la liaison (mesuré : ~90 messages/s), assez peu pour que la file se
# vide en quelques secondes.
MQTT_MAX_EN_ATTENTE = int(os.getenv("MQTT_MAX_EN_ATTENTE", "200"))
# Attente maximale de l'acquittement des messages restants avant de déclarer un
# fichier traité (cf. attendre_publications_terminees()).
DRAIN_TIMEOUT = float(os.getenv("DRAIN_TIMEOUT", "120"))
# Délai au-delà duquel on cesse d'attendre une place et on bascule sur SQLite
# (évite de bloquer indéfiniment si le broker cesse d'acquitter sans couper).
MQTT_ATTENTE_TIMEOUT = float(os.getenv("MQTT_ATTENTE_TIMEOUT", "60"))

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
# Partitionnement multi-processus : RETIRÉ le 07/08/2026.
#
# Une version antérieure permettait de lancer N processus d'ingestion sur le
# même dossier (SHARD_INDEX / SHARD_COUNT, répartition par crc32 du nom de
# fichier) pour accélérer le rattrapage des .dxd historiques. Mesuré côté VPS,
# ce partitionnement n'apportait RIEN : 4,68 Mbit/s avec 1 processus contre
# 4,85 Mbit/s avec 4 processus. Le plafond n'était ni le CPU d'Amiens
# (25-60 % d'occupation) ni sa liaison montante (13,7 Mbit/s atteints avec de
# grosses charges utiles), mais le coût PAR MESSAGE — donc un plafond partagé
# entre tous les processus, que multiplier les processus ne pouvait pas lever.
#
# La publication par lots (cf. DXD_BATCH_SIZE) s'attaque directement à cette
# cause et rend un seul processus largement suffisant : 54 246 échantillons/s
# mesurés, contre ~1 020 auparavant. Le partitionnement a donc été supprimé
# plutôt que conservé « au cas où » — il ajoutait de la complexité (dont un
# verrou capteurs_retrait.json non valable entre processus) sans gain mesuré.
# ---------------------------------------------------------------------------
# Publication par LOTS (07/08/2026, cf. logique_projet.md).
#
# PROBLÈME MESURÉ : le format « un message MQTT par échantillon » pèse ~520
# octets pour transporter un seul flottant, dont ~490 de métadonnées
# (fichier, mur, couche, position, canal, unité, fréquence, horodatage ISO)
# strictement IDENTIQUES pour tous les échantillons d'un canal d'un fichier.
# Le débit Amiens→VPS plafonnait alors à ~1 020 échantillons/s, non pas par
# manque de bande passante (le même PC atteint ~13,7 Mbit/s avec de grosses
# charges utiles) ni de CPU (25-60 % d'occupation), mais à cause du COÛT PAR
# MESSAGE. Preuve : lancer 4 processus d'ingestion en parallèle ne changeait
# rien (4,85 contre 4,68 Mbit/s mesurés côté VPS) — le plafond est partagé,
# pas par processus. C'est pourquoi la parallélisation côté VPS (3 replicas
# de bridge, souscriptions partagées, HPA à 6 consumers) n'avait rien donné :
# elle traitait un goulot d'étranglement qui n'existait pas.
#
# CORRECTIF : un message par LOT d'échantillons d'un même canal. Les
# métadonnées constantes sortent du lot (une fois par message au lieu d'une
# fois par échantillon) et les horodatages deviennent t0 + k*dt. Coût mesuré :
# ~28,6 octets/échantillon, soit 54 246 échantillons/s Amiens→VPS contre
# ~1 020 avant (×53).
#
# DXD_BATCH_SIZE : 600 échantillons ≈ 17 ko/message — largement sous toutes
# les limites de la chaîne (Mosquitto message_size_limit, Kafka
# max.message.bytes à 1 Mio, max_request_size du producteur). Monter plus
# haut ne gagne presque rien (28,6 contre 31,9 octets/échantillon entre 600
# et 100) tout en alourdissant la mémoire et la granularité de reprise.
# ---------------------------------------------------------------------------
DXD_BATCH_SIZE = int(os.getenv("DXD_BATCH_SIZE", "600"))
# Version du format de charge utile — le consumer s'en sert pour distinguer
# un lot d'un message point-par-point de l'ancien format.
FORMAT_LOT = 2
# Écart maximal toléré à la grille t0 + k*dt avant de couper le lot. Les
# fichiers réels restent à ~5.6e-8 s ; 1 µs correspond à la précision à
# laquelle l'ancien format arrondissait déjà (timedelta → microsecondes).
TOLERANCE_PAS_S = float(os.getenv("TOLERANCE_PAS_S", "1e-6"))

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

# Compteur de messages QoS 1 publiés mais pas encore acquittés par le broker
# (cf. MQTT_MAX_EN_ATTENTE) — incrémenté avant publish(), décrémenté par
# on_publish(). Protégé par une Condition pour endormir le producteur quand
# le plafond est atteint plutôt que de le laisser saturer la file de paho.
_en_attente: int = 0
_cond_en_attente = threading.Condition()
# Statistiques d'observabilité, affichées en fin de fichier.
_nb_publies: int = 0
_nb_bufferises: int = 0


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


def _reserver_place() -> bool:
    """Attendre qu'une place se libère parmi les messages QoS 1 en attente.

    Returns:
        True si une place a été réservée (le compteur a été incrémenté et
        devra être décrémenté par on_publish ou par _liberer_place), False
        s'il faut basculer sur SQLite (déconnexion ou attente trop longue).
    """
    global _en_attente
    debut = time.monotonic()
    with _cond_en_attente:
        while _en_attente >= MQTT_MAX_EN_ATTENTE:
            if not _mqtt_connecte:
                return False
            if time.monotonic() - debut > MQTT_ATTENTE_TIMEOUT:
                print(
                    f"⚠️  {MQTT_MAX_EN_ATTENTE} messages QoS 1 toujours non "
                    f"acquittés après {MQTT_ATTENTE_TIMEOUT:.0f}s — "
                    "bascule SQLite."
                )
                return False
            _cond_en_attente.wait(0.5)
        _en_attente += 1
        return True


def _liberer_place() -> None:
    """Rendre une place réservée pour un message qui n'a pas été accepté."""
    global _en_attente
    with _cond_en_attente:
        _en_attente -= 1
        _cond_en_attente.notify_all()


def attendre_publications_terminees(timeout: float | None = None) -> int:
    """Attendre l'acquittement de tous les messages QoS 1 encore en vol.

    À appeler avant de considérer un fichier .dxd comme traité : publish() ne
    fait qu'empiler dans la file interne de paho, donc au retour de
    l'extraction une partie des messages n'a pas encore quitté la machine.
    Sans cette attente, « fichier déplacé dans traites/ » ne signifie pas
    « données arrivées chez le broker », et tout arrêt du processus perd
    silencieusement la file — ce qui s'est produit lors du test du 07/08/2026.

    Args:
        timeout: Attente maximale en secondes (défaut : DRAIN_TIMEOUT).

    Returns:
        Nombre de messages encore non acquittés (0 = tout est parti).
    """
    limite = time.monotonic() + (DRAIN_TIMEOUT if timeout is None else timeout)
    with _cond_en_attente:
        while _en_attente > 0 and time.monotonic() < limite:
            _cond_en_attente.wait(0.5)
        return _en_attente


def publier_ou_stocker(topic: str, payload: dict) -> None:
    """Publier sur MQTT cloud ou stocker localement si cloud indisponible.

    Applique une contre-pression : au plus MQTT_MAX_EN_ATTENTE messages
    QoS 1 non acquittés simultanément. Sans cela, la boucle d'extraction
    devance largement le réseau, épuise l'espace d'identifiants de message
    de paho (16 bits) et fait échouer toutes les publications suivantes,
    qui finissent alors dans SQLite alors que le cloud est joignable
    (cf. commentaire de MQTT_MAX_EN_ATTENTE).
    """
    global _nb_publies, _nb_bufferises
    payload_json = json.dumps(payload)
    if _mqtt_connecte and _reserver_place():
        try:
            result = mqtt_client.publish(topic, payload_json, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                _nb_publies += 1
                return
            # Aucun on_publish ne viendra pour ce message : rendre la place.
            _liberer_place()
            print(f"⚠️  Publication MQTT refusée (rc={result.rc}) — stockage SQLite.")
        except Exception as exc:
            _liberer_place()
            print(f"⚠️  Publication MQTT échouée ({exc}) — stockage SQLite.")
    stocker_localement(topic, payload_json)
    _nb_bufferises += 1


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


def on_publish(client, userdata, mid) -> None:
    """Libérer une place de contre-pression à chaque acquittement QoS 1.

    S'exécute sur le thread réseau de paho : ne doit rien faire d'autre que
    décrémenter le compteur et réveiller le producteur éventuellement endormi.
    """
    global _en_attente
    with _cond_en_attente:
        if _en_attente > 0:
            _en_attente -= 1
        if _en_attente < MQTT_MAX_EN_ATTENTE:
            _cond_en_attente.notify_all()


def on_disconnect(client, userdata, rc) -> None:
    global _mqtt_connecte, _en_attente
    _mqtt_connecte = False
    # La session est propre (clean_session par défaut) : les messages encore
    # en attente ne seront jamais acquittés. Remettre le compteur à zéro et
    # réveiller le producteur, sinon il resterait bloqué à attendre des
    # acquittements qui ne viendront plus.
    with _cond_en_attente:
        _en_attente = 0
        _cond_en_attente.notify_all()
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
            # Réserver une place comme le fait publier_ou_stocker() : sinon
            # on_publish() libérerait une place jamais réservée et
            # relâcherait d'autant la contre-pression de l'extraction.
            if not _reserver_place():
                break
            try:
                result = mqtt_client.publish(topic, payload, qos=1)
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    _liberer_place()
                    erreurs += 1
                    continue
                result.wait_for_publish(timeout=5.0)
                with sqlite3.connect(SQLITE_BUFFER_FILE) as conn:
                    conn.execute("DELETE FROM buffer_mqtt WHERE id = ?", (row_id,))
                    conn.commit()
                envoyes += 1
                time.sleep(0.05)  # Pause légère pour ne pas saturer le broker.
            except Exception as exc:
                _liberer_place()
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

def decouper_lot(timestamps, debut: int, fin_max: int) -> int:
    """Déterminer où s'arrête un lot à pas d'échantillonnage constant.

    Le format par lot encode les horodatages sous la forme t0 + k*dt au lieu
    d'une chaîne ISO par échantillon : c'est ce qui fait tomber le coût de
    ~520 à ~29 octets/échantillon. Cet encodage n'est exact que si le pas est
    réellement constant sur toute la durée du lot — on ne le SUPPOSE donc pas,
    on le VÉRIFIE, et on coupe le lot dès qu'un échantillon s'écarte de la
    grille de plus de TOLERANCE_PAS_S.

    Mesuré sur les fichiers réels (949 fichiers, 8 canaux, 10 Hz) : un seul
    pas distinct (0.1 s) et un écart maximal à la grille de 5.6e-8 s sur une
    fenêtre de 600 échantillons — très en-dessous de la microseconde à
    laquelle l'ancien format arrondissait déjà. Un fichier comportant un vrai
    trou serait simplement découpé en plusieurs lots, sans perte de précision.

    Args:
        timestamps: Horodatages DeweSoft (secondes) du canal courant.
        debut: Index du premier échantillon du lot.
        fin_max: Borne supérieure exclusive souhaitée pour le lot.

    Returns:
        Index de fin (exclu) effectif du lot : fin_max, ou plus tôt si le pas
        d'échantillonnage cesse d'être constant.
    """
    if fin_max - debut <= 2:
        return fin_max

    t0 = timestamps[debut]
    dt = timestamps[debut + 1] - t0
    if dt <= 0:
        # Pas exploitable (horodatages identiques ou décroissants) : lot d'un
        # seul échantillon, le consumer le datera avec t0 seul.
        return debut + 1

    for k in range(2, fin_max - debut):
        if abs(timestamps[debut + k] - (t0 + k * dt)) > TOLERANCE_PAS_S:
            return debut + k
    return fin_max


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
            # ---------------------------------------------------------------
            # Origine des horodatages relatifs (07/08/2026, cf.
            # logique_projet.md).
            #
            # DeweSoftX enregistre ici UNE acquisition continue découpée en
            # segments de 12 h. Les horodatages relatifs renvoyés par
            # DWIGetScaledSamples sont comptés depuis le début de la SESSION
            # d'acquisition, pas depuis le début du fichier : le premier
            # échantillon d'un segment vaut donc 0, 43 200, 604 800, 691 200 s…
            # (toujours un multiple de 43 200 s = 12 h), selon le rang du
            # segment dans la session.
            #
            # start_store_time, lui, correspond bien au début DE CE FICHIER
            # (vérifié : il coïncide exactement avec la date/heure du nom de
            # fichier sur tous les fichiers échantillonnés).
            #
            # Additionner les deux comptait donc le décalage DEUX FOIS et
            # datait les mesures de plusieurs jours dans le futur — mesuré sur
            # Mur_terlian_2026_06_18_092618.dxd : t[0] = 604 800 s, donc les
            # points étaient écrits au 25/06 au lieu du 18/06, soit pile
            # 7 jours trop tard. Pire, ce décalage étant un multiple entier de
            # 12 h, les points d'un fichier venaient ÉCRASER silencieusement
            # ceux d'un autre segment (même mesure, mêmes tags, même _time).
            #
            # On ramène donc l'origine au premier échantillon réellement
            # stocké dans le fichier. Le minimum est pris sur l'ensemble des
            # canaux pour préserver leur alignement relatif si l'un d'eux
            # démarrait plus tard que les autres.
            # ---------------------------------------------------------------
            premiers_horodatages = []
            for k in range(ch_count.value):
                cnt_k = ctypes.c_longlong()
                check_error(
                    lib,
                    lib.DWIGetScaledSamplesCount(
                        reader, channel_list[k].index, ctypes.byref(cnt_k)
                    ),
                )
                if cnt_k.value == 0:
                    continue
                un = ctypes.c_longlong(1)
                ech_1 = (ctypes.c_double * 1)()
                hor_1 = (ctypes.c_double * 1)()
                check_error(
                    lib,
                    lib.DWIGetScaledSamples(
                        reader, channel_list[k].index, 0, un, ech_1, hor_1
                    ),
                )
                premiers_horodatages.append(hor_1[0])

            decalage_session = min(premiers_horodatages) if premiers_horodatages else 0.0
            if decalage_session:
                print(
                    f"   🕒 Origine relative recalée : premier échantillon à "
                    f"t={decalage_session:.0f}s ({decalage_session / 86400:.2f} j) "
                    f"→ ramené à {horodatage_debut.isoformat()}"
                )

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

                # Métadonnées identiques pour TOUS les échantillons de ce canal
                # dans ce fichier : sorties du lot une seule fois (cf.
                # DXD_BATCH_SIZE) au lieu d'être répétées à chaque échantillon.
                meta_canal = {
                    "source": "dewesoft",
                    "methode": "import_dxd_batch",
                    "format": FORMAT_LOT,
                    "fichier_source": nom_fichier,
                    "canal_index": i,
                    "canal_nom": nom_canal,
                    "canal_unite": unite,
                    "nom_mur": infos_canal.get("nom_mur", ""),
                    "nom_couche": infos_canal.get("nom_couche", ""),
                    "position": infos_canal.get("position", ""),
                    "categorie R&D": infos_canal.get("categorie R&D", ""),
                    "horodatage": horodatage_lecture,
                    "taux_echantillonnage": taux,
                }

                total = sample_cnt.value
                debut_lot = 0
                while debut_lot < total:
                    fin_lot = decouper_lot(
                        timestamps, debut_lot, min(debut_lot + DXD_BATCH_SIZE, total)
                    )
                    n_lot = fin_lot - debut_lot

                    # Horodatage du 1er échantillon du lot, arrondi à la
                    # microseconde comme le faisait timedelta() dans l'ancien
                    # format point-par-point — la reconstitution côté consumer
                    # (t0_ns + k*dt_ns) redonne donc exactement les mêmes
                    # instants qu'avant, à la microseconde près.
                    t0 = horodatage_debut + timedelta(
                        seconds=timestamps[debut_lot] - decalage_session
                    )
                    t0_ns = int(round(t0.timestamp() * 1_000_000)) * 1_000
                    dt_ns = (
                        int(round((timestamps[debut_lot + 1] - timestamps[debut_lot]) * 1e9))
                        if n_lot > 1
                        else 0
                    )

                    payload = dict(meta_canal)
                    payload["t0_ns"] = t0_ns
                    payload["dt_ns"] = dt_ns
                    payload["n"] = n_lot
                    payload["horodatage_dewesoft"] = timestamps[debut_lot]
                    payload["valeurs"] = [samples[k] for k in range(debut_lot, fin_lot)]
                    payload["valeurs_filtrees"] = valeurs_filtrees[debut_lot:fin_lot]
                    payload["indices_aberrants"] = [
                        k - debut_lot
                        for k in range(debut_lot, fin_lot)
                        if k in indices_aberrants
                    ]

                    publier_ou_stocker(MQTT_TOPIC_DEWESOFT, payload)
                    nb_mesures += n_lot
                    debut_lot = fin_lot

                print(f"   ... {nom_canal} : {total} mesure(s) publiées")

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
            debut_fichier = time.monotonic()
            try:
                nb_canaux, nb_mesures = extraire_et_publier(chemin)
                # publish() ne fait qu'empiler dans la file interne de paho :
                # au retour d'extraire_et_publier(), une partie des messages
                # n'est pas encore partie sur le réseau, et AUCUN n'est encore
                # forcément acquitté par le broker. Déplacer le fichier dans
                # "traites" à cet instant reviendrait à le déclarer traité
                # alors qu'un arrêt du processus perdrait tout ce qui reste en
                # file. Mesuré le 07/08/2026 : sur un import de 2 fichiers
                # (11 520 messages de lot), 6 100 messages ont été perdus
                # exactement de cette façon — le fichier était marqué traité,
                # puis le processus arrêté, et 47 % des points n'étaient jamais
                # arrivés dans InfluxDB.
                restants = attendre_publications_terminees()
                duree = time.monotonic() - debut_fichier
                if restants:
                    print(
                        f"⚠️  {nom} : {restants} message(s) toujours non acquittés "
                        f"après {DRAIN_TIMEOUT}s — fichier laissé en place pour "
                        "être retenté au prochain passage."
                    )
                    continue
                print(
                    f"✅ {nom} : {nb_canaux} canal(aux), {nb_mesures} mesure(s) "
                    f"en {duree:.0f}s ({nb_mesures / duree:.0f} mesures/s) — "
                    f"{_nb_publies} publiées MQTT, {_nb_bufferises} bufferisées SQLite."
                )
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
    print(f"  Publication par lots : {DXD_BATCH_SIZE} échantillons/message")

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
    mqtt_client.on_publish = on_publish
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)
    mqtt_client.max_inflight_messages_set(MQTT_MAX_INFLIGHT)

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
