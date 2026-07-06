"""
Ingestion temps réel des données DeweSoftX — MurMetric / FRD-CODEM.

Ce script se connecte à une instance DeweSoftX active via la bibliothèque
DSRemoteConnect (mode NET) et publie les mesures de chaque canal (capteurs
de retrait, déformation, etc.) sur le broker MQTT cloud.

En cas d'indisponibilité du broker MQTT (perte internet, VPS down), les
mesures sont stockées dans le buffer SQLite local partagé avec
``ingestion_capteurs_bluetooth.py`` (murmetric_buffer.db), puis renvoyées
automatiquement à la reconnexion.

Prérequis :
    1. DeweSoftX installé et en cours d'exécution sur ce PC.
    2. DeweSoftX configuré avec le serveur "Dewesoft NET" activé
       (Options → Network → Dewesoft NET, port 8999).
    3. Fichier DSRemoteConnect64.dll placé dans le même dossier que ce script.
       Télécharger depuis : https://dewesoft.com/download/developer-downloads
       (DSRemoteConnect v1.1.0 → dossier DSRemotePython/)
    4. Fichier de setup (.dxs) DeweSoftX configuré avec les canaux de retrait.

Architecture :
    DeweSoftX (mesure active)
        │  DSRemoteConnect NET (port 8999)
        ▼
    ingestion_dewesoft.py
        │  publier_ou_stocker()
        ├─ ☁️ MQTT cloud disponible → publish direct
        └─ 💾 MQTT cloud indisponible → SQLite local (murmetric_buffer.db)

Topic MQTT : frd/dewesoft/bruts
    Une mesure par canal et par lecture (payload JSON).

Usage :
    python -u ingestion_dewesoft.py

    Variables d'environnement reconnues :
        MQTT_BROKER         Adresse du broker MQTT cloud (défaut : localhost)
        MQTT_PORT           Port MQTT                    (défaut : 1883)
        DSX_HOST            Hôte DeweSoftX               (défaut : localhost)
        DSX_PORT_DATA       Port données NET             (défaut : 8999)
        DSX_PORT_CONTROL    Port contrôle NET            (défaut : 8001)
        DSX_SETUP_FILE      Chemin fichier .dxs          (optionnel)
        POLL_INTERVAL       Intervalle de lecture en secondes (défaut : 1.0)
        DLL_PATH            Chemin complet vers DSRemoteConnect64.dll
"""

import ctypes
import json
import os
import sqlite3
import sys
import time
from ctypes import HANDLE, POINTER, byref, c_char, c_char_p, c_double, c_int, c_size_t, pointer
from datetime import datetime

import paho.mqtt.client as mqtt

# Encodage UTF-8 sous Windows (console cp1252 par défaut).
sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Configuration MQTT — surchargeable via variables d'environnement.
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_DEWESOFT = os.getenv("MQTT_TOPIC_DEWESOFT", "frd/dewesoft/bruts")

# ---------------------------------------------------------------------------
# Configuration DeweSoftX — surchargeable via variables d'environnement.
# ---------------------------------------------------------------------------
DSX_HOST = os.getenv("DSX_HOST", "localhost")
DSX_PORT_DATA = int(os.getenv("DSX_PORT_DATA", "8999"))
DSX_PORT_CONTROL = int(os.getenv("DSX_PORT_CONTROL", "8001"))
DSX_SETUP_FILE = os.getenv("DSX_SETUP_FILE", "")

# Fichier de setup optionnel — si vide, DeweSoftX utilise sa config courante.
CHAINE_CONNEXION = f"{DSX_HOST}:{DSX_PORT_DATA}:{DSX_PORT_CONTROL}"

# Intervalle entre deux lectures de données (secondes).
# Avec 8 canaux à 1 mesure/s, 1 s est le minimum utile.
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))

# Taille maximale du buffer de données par canal par lecture.
# 8 mesures/s × 10 s de marge = 80 échantillons max par canal.
BUFFER_SAMPLES = 1000

# ---------------------------------------------------------------------------
# Chemin vers la DLL DSRemoteConnect (64 bits).
# ---------------------------------------------------------------------------
DLL_PATH = os.getenv(
    "DLL_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "DSRemoteConnect64.dll"),
)

# ---------------------------------------------------------------------------
# Configuration buffer SQLite local (résilience cloud) — partagé avec
# ingestion_capteurs_bluetooth.py.
# ---------------------------------------------------------------------------
SQLITE_BUFFER_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "murmetric_buffer.db"
)

# ---------------------------------------------------------------------------
# État de la connexion MQTT.
# ---------------------------------------------------------------------------
_mqtt_connecte: bool = False


# ===========================================================================
# Buffer SQLite local (même structure que ingestion_capteurs_bluetooth.py).
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
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_horodatage
            ON buffer_mqtt (horodatage)
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


# ===========================================================================
# Publication MQTT avec basculement SQLite.
# ===========================================================================

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
# Callbacks MQTT.
# ===========================================================================

def on_connect(client, userdata, flags, rc) -> None:
    """Gérer la connexion au broker MQTT cloud."""
    global _mqtt_connecte
    if rc == 0:
        _mqtt_connecte = True
        print(f"✅ Connecté au Broker MQTT cloud ({MQTT_BROKER}:{MQTT_PORT})")
    else:
        print(f"❌ Connexion MQTT cloud refusée (code {rc})")


def on_disconnect(client, userdata, rc) -> None:
    """Gérer la déconnexion du broker MQTT cloud."""
    global _mqtt_connecte
    _mqtt_connecte = False
    if rc != 0:
        print(
            f"⚠️  Déconnecté du Broker MQTT cloud (rc={rc}) "
            "— basculement sur SQLite local."
        )


# ===========================================================================
# Wrapper DSRemoteConnect — API DeweSoftX via ctypes.
# ===========================================================================

class DSRemoteConnect:
    """Wrapper Python de la DLL DSRemoteConnect64.dll (Dewesoft).

    Encapsule les appels ctypes vers la DLL et expose une interface
    simple pour les opérations courantes : connexion, enumération des
    canaux, lecture de données, déconnexion.

    Attributes:
        dll:           Instance WinDLL de la bibliothèque DSRemoteConnect.
        instance:      Handle de l'instance DSRemoteConnect.
        canaux:        Liste des handles de canaux créés.
        infos_canaux:  Liste des métadonnées (nom, unité, taux, type) par canal.
    """

    def __init__(self, dll_path: str) -> None:
        """Charger la DLL et initialiser les attributs.

        Args:
            dll_path: Chemin absolu vers DSRemoteConnect64.dll.

        Raises:
            FileNotFoundError: Si la DLL est introuvable.
            OSError: Si le chargement de la DLL échoue.
        """
        if not os.path.exists(dll_path):
            raise FileNotFoundError(
                f"DLL DSRemoteConnect introuvable : {dll_path}\n"
                "Télécharger depuis https://dewesoft.com/download/developer-downloads"
            )
        self.dll = ctypes.WinDLL(dll_path)
        self.instance = HANDLE()
        self.canaux: list[HANDLE] = []
        self.infos_canaux: list[dict] = []

    def _verifier(self, code: int, operation: str) -> int:
        """Vérifier le code de retour d'un appel DLL et logger les erreurs.

        Args:
            code:      Code de retour de la fonction DLL (< 0 = erreur).
            operation: Nom de l'opération pour le message d'erreur.

        Returns:
            Le code de retour inchangé.
        """
        if code < 0:
            print(f"⚠️  DSRemoteConnect — {operation} : erreur code {code}")
        return code

    def connecter(self, chaine_connexion: str) -> None:
        """Créer une instance et se connecter à DeweSoftX en mode NET.

        Args:
            chaine_connexion: Chaîne au format "host:port_data:port_control".
                              Exemple : "localhost:8999:8001"

        Raises:
            RuntimeError: Si la création de l'instance ou la connexion échoue.
        """
        # Mode 1 = connexion réseau (NET), nécessite DeweSoftX Dewesoft NET activé.
        code = self.dll.dsconCreateInstance(pointer(self.instance), 1)
        if self._verifier(code, "dsconCreateInstance") < 0:
            raise RuntimeError("Impossible de créer l'instance DSRemoteConnect.")

        buf = ctypes.create_string_buffer(chaine_connexion.encode())
        code = self.dll.dsconConnect(self.instance, buf)
        if self._verifier(code, "dsconConnect") < 0:
            raise RuntimeError(
                f"Connexion à DeweSoftX impossible ({chaine_connexion}). "
                "Vérifiez que DeweSoftX tourne avec 'Dewesoft NET' activé."
            )
        print(f"✅ Connecté à DeweSoftX ({chaine_connexion})")

    def charger_setup(self, chemin: str) -> None:
        """Charger un fichier de setup DeweSoftX (.dxs).

        Args:
            chemin: Chemin absolu vers le fichier .dxs.
        """
        self._verifier(
            self.dll.dsconLoadSetup(self.instance, c_char_p(chemin.encode())),
            f"dsconLoadSetup({chemin})",
        )

    def enumerer_canaux(self) -> None:
        """Lister tous les canaux disponibles et créer leurs instances.

        Remplit ``self.canaux`` et ``self.infos_canaux`` avec les handles
        et métadonnées (nom, unité, taux d'échantillonnage, type) de chaque
        canal exposé par DeweSoftX.
        """
        nb = c_size_t()
        self._verifier(
            self.dll.dsconGetChannelCount(self.instance, byref(nb)),
            "dsconGetChannelCount",
        )
        nombre = nb.value
        print(f"📋 {nombre} canal(aux) détecté(s) dans DeweSoftX.")

        if nombre == 0:
            return

        # Enumération des identifiants de canaux (tableau de pointeurs).
        labels_ptr = pointer((POINTER(c_char_p) * nombre)())
        local_val = c_size_t(nombre)
        self.dll.dsconEnumerateChannels(self.instance, labels_ptr, byref(local_val))

        buf_nom = ctypes.cast((c_char * 100)(), c_char_p)
        buf_unit = ctypes.cast((c_char * 50)(), c_char_p)

        for i in range(nombre):
            # Création du handle de canal.
            raw_id = labels_ptr[0][0][i]
            buf_id = (c_char * len(raw_id))()
            ctypes.cast(buf_id, c_char_p).value = raw_id
            ch_handle = HANDLE()
            self.dll.dsconCreateChannelInstance(
                self.instance, ctypes.cast(buf_id, c_char_p), pointer(ch_handle)
            )
            self.canaux.append(ch_handle)

            # Récupération des métadonnées du canal.
            self.dll.dsconChannelGetName(ch_handle, buf_nom, 100)
            nom = buf_nom.value.decode("utf-8", errors="replace") if buf_nom.value else f"Canal_{i}"

            self.dll.dsconGetChUnit(ch_handle, buf_unit, 50)
            unite = buf_unit.value.decode("utf-8", errors="replace") if buf_unit.value else ""

            taux = c_double(0.0)
            self.dll.dsconGetSampleRate(ch_handle, byref(taux))

            ch_type = c_int(0)
            self.dll.dsconGetChannelType(ch_handle, byref(ch_type))

            infos = {
                "index": i,
                "nom": nom,
                "unite": unite,
                "taux_echantillonnage": taux.value,
                "type": ch_type.value,
            }
            self.infos_canaux.append(infos)
            print(
                f"   Canal {i:2d} : {nom:30s} | unité : {unite:10s} "
                f"| {taux.value:.1f} Hz | type {ch_type.value}"
            )

    def demarrer(self) -> None:
        """Démarrer l'acquisition dans DeweSoftX."""
        self._verifier(
            self.dll.dsconStartMeasurement(self.instance), "dsconStartMeasurement"
        )
        print("▶  Acquisition DeweSoftX démarrée.")

    def lire_donnees(
        self,
        canal_idx: int,
        buffer_size: int = BUFFER_SAMPLES,
    ) -> tuple[list[float], list[float]]:
        """Lire les données disponibles pour un canal depuis la dernière lecture.

        Args:
            canal_idx:   Index du canal dans ``self.canaux``.
            buffer_size: Taille maximale du buffer de lecture.

        Returns:
            Tuple (valeurs, horodatages) en float. Listes vides si pas de données.
        """
        data = ctypes.cast((c_double * buffer_size)(), POINTER(c_double))
        timestamps = ctypes.cast((c_double * buffer_size)(), POINTER(c_double))
        count = c_size_t(buffer_size)

        code = self.dll.dsconChannelReadScalarData_2(
            self.canaux[canal_idx], data, timestamps, byref(count)
        )
        if code < 0 or count.value == 0:
            return [], []

        return (
            [data[i] for i in range(count.value)],
            [timestamps[i] for i in range(count.value)],
        )

    def arreter(self) -> None:
        """Arrêter l'acquisition dans DeweSoftX."""
        self._verifier(
            self.dll.dsconStopMeasurement(self.instance), "dsconStopMeasurement"
        )
        print("⏹  Acquisition DeweSoftX arrêtée.")

    def deconnecter(self) -> None:
        """Libérer les canaux, déconnecter et détruire l'instance."""
        for i, ch in enumerate(self.canaux):
            self._verifier(
                self.dll.dsconFreeChannelInstance(ch),
                f"dsconFreeChannelInstance(canal {i})",
            )
        self.canaux.clear()
        self._verifier(self.dll.dsconDisconnect(self.instance), "dsconDisconnect")
        self._verifier(self.dll.dsconDestroyInstance(self.instance), "dsconDestroyInstance")
        print("🔌 DSRemoteConnect déconnecté.")


# ===========================================================================
# Boucle principale d'ingestion.
# ===========================================================================

def boucle_ingestion(dsx: DSRemoteConnect) -> None:
    """Lire et publier les données de tous les canaux DeweSoftX en continu.

    Sonde tous les canaux toutes les POLL_INTERVAL secondes. Pour chaque
    mesure disponible, construit un payload JSON et appelle
    ``publier_ou_stocker()`` (MQTT cloud ou SQLite local selon disponibilité).

    Le payload publié sur ``frd/dewesoft/bruts`` a la structure :
    {
        "source":             "dewesoft",
        "canal_index":        0,
        "canal_nom":          "VA1",
        "canal_unite":        "mm/m",
        "valeur":             0.125,
        "horodatage_dewesoft": 1234567890.123,
        "horodatage":         "02/07/2026 10:45:32",
        "taux_echantillonnage": 1.0
    }

    Args:
        dsx: Instance DSRemoteConnect connectée et avec canaux initialisés.
    """
    print(
        f"\n🔁 Ingestion en cours — {len(dsx.canaux)} canal(aux), "
        f"poll toutes les {POLL_INTERVAL} s. Arrêt : Ctrl+C\n"
    )

    while True:
        debut = time.monotonic()
        horodatage_lecture = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        total_publies = 0

        for idx, infos in enumerate(dsx.infos_canaux):
            valeurs, timestamps = dsx.lire_donnees(idx)

            for val, ts in zip(valeurs, timestamps):
                payload = {
                    "source": "dewesoft",
                    "canal_index": idx,
                    "canal_nom": infos["nom"],
                    "canal_unite": infos["unite"],
                    "valeur": val,
                    "horodatage_dewesoft": ts,
                    "horodatage": horodatage_lecture,
                    "taux_echantillonnage": infos["taux_echantillonnage"],
                }
                publier_ou_stocker(MQTT_TOPIC_DEWESOFT, payload)
                total_publies += 1

        if total_publies > 0:
            dest = "☁️  MQTT cloud" if _mqtt_connecte else "💾 SQLite local"
            print(
                f"[{horodatage_lecture}] {total_publies} mesure(s) publiées "
                f"→ {dest}"
            )

        # Attendre le reste de l'intervalle de polling.
        elapsed = time.monotonic() - debut
        sleep_time = max(0.0, POLL_INTERVAL - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)


# ===========================================================================
# Point d'entrée.
# ===========================================================================

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")

    print("=" * 60)
    print("  MurMetric — Ingestion DeweSoftX (capteur retrait)")
    print("=" * 60)

    # --- Initialisation SQLite ---
    initialiser_sqlite()

    # --- Connexion MQTT cloud ---
    mqtt_client = mqtt.Client()
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

    # --- Connexion DeweSoftX ---
    dsx = DSRemoteConnect(DLL_PATH)
    try:
        dsx.connecter(CHAINE_CONNEXION)

        if DSX_SETUP_FILE:
            dsx.charger_setup(DSX_SETUP_FILE)

        dsx.enumerer_canaux()

        if not dsx.canaux:
            print("❌ Aucun canal disponible dans DeweSoftX. Vérifiez le setup.")
            sys.exit(1)

        dsx.demarrer()

        # Courte pause pour laisser DeweSoftX démarrer l'acquisition.
        time.sleep(1.0)

        boucle_ingestion(dsx)

    except KeyboardInterrupt:
        print("\n🛑 Arrêt demandé par l'utilisateur.")
    except Exception as exc:
        print(f"❌ Erreur fatale : {exc}")
    finally:
        try:
            dsx.arreter()
            dsx.deconnecter()
        except Exception:
            pass
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print("👋 ingestion_dewesoft.py terminé.")
