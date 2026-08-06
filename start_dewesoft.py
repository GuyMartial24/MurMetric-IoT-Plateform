"""
Point d'entrée de l'ingestion DeweSoftX — PC labo Windows, FRD-CODEM.

Ce script est destiné à être exécuté sur le PC Windows hébergeant
DeweSoftX. Lance ``ingestion_dewesoft_dxd.py`` (dépôt de fichiers .dxd) —
seule méthode d'ingestion retenue (licence "Dewesoft NET" nécessaire au
streaming live jugée trop coûteuse pour l'usage prévu, décision du
23/07/2026 ; la méthode live/COM (``ingestion_dewesoft.py``) a été
définitivement abandonnée et retirée du dépôt le 04/08/2026 — cf.
logique_projet.md).

Note architecturale
-------------------
MurMetric est déployé sur deux machines distinctes :

  - Raspberry Pi (Amiens) : capteurs BLE température/humidité
    → lancé par ``start.py``
  - PC labo Windows (Amiens) : capteur retrait via DeweSoftX
    → lancé par ce fichier ``start_dewesoft.py``

Chaque machine gère son propre buffer SQLite (``murmetric_buffer.db``)
indépendamment. Aucun partage de fichier entre les deux machines n'est
nécessaire : chacune publie directement sur le broker MQTT cloud et
bufferise localement si le réseau est indisponible.

DeweSoftX dépose/exporte ses fichiers .dxd dans DXD_WATCH_FOLDER, un
dossier LOCAL à ce PC (aucun composant réseau supplémentaire, pas de
dépendance à docker-compose.yml/VPS) ; ``ingestion_dewesoft_dxd.py`` le
surveille et publie sur le pipeline MQTT → Kafka → InfluxDB.

DXD_WATCH_FOLDER reste une variable d'environnement, jamais codée en dur
dans ``ingestion_dewesoft_dxd.py`` — ce script-ci ne fait que lui fournir
une valeur par défaut (DXD_WATCH_FOLDER_DEFAUT ci-dessous) si elle n'est
pas déjà définie dans l'environnement. Au moment de la mise en prod,
changer le dossier surveillé se fait donc de deux façons, sans toucher au
code d'ingestion lui-même :
  - définir DXD_WATCH_FOLDER dans l'environnement avant de lancer ce
    script (prioritaire) ;
  - ou modifier la seule constante DXD_WATCH_FOLDER_DEFAUT ci-dessous.

Usage :
    python start_dewesoft.py

    Variables d'environnement reconnues :
        DXD_WATCH_FOLDER    Dossier .dxd surveillé
                            (défaut : cf. DXD_WATCH_FOLDER_DEFAUT ci-dessous)
        MQTT_BROKER         Adresse du broker MQTT cloud   (défaut : localhost)
        MQTT_PORT           Port MQTT                      (défaut : 1883)
        SQLITE_RETENTION    Rétention buffer local (jours) (défaut : 7)
        SYNC_BATCH_SIZE     Messages par batch de rattrapage (défaut : 50)
        SYNC_INTERVAL       Intervalle sync SQLite (s)     (défaut : 30)

        Réglages propres à ingestion_dewesoft_dxd.py :
        DXD_PROCESSED_FOLDER, DXD_ERROR_FOLDER, POLL_INTERVAL_DXD,
        STABLE_CHECKS, HAMPEL_FENETRE, HAMPEL_SEUIL_K.
"""

import os
import subprocess
import sys

sys.stdout.reconfigure(encoding="utf-8")

BASE = os.path.dirname(os.path.abspath(__file__))

# Dossier de dépôt des .dxd si DXD_WATCH_FOLDER n'est pas déjà défini dans
# l'environnement — à adapter ici au moment de la mise en prod (ou, mieux,
# en définissant DXD_WATCH_FOLDER dans l'environnement, qui reste prioritaire
# sur cette valeur). Dossier réel où DeweSoftX génère ses .dxd sur le PC
# labo Windows d'Amiens (confirmé le 04/08/2026, cf. logique_projet.md).
DXD_WATCH_FOLDER_DEFAUT = r"C:\Users\Public\Documents\Dewesoft\Data"


if __name__ == "__main__":
    print("=" * 60)
    print("  MurMetric — Ingestion DeweSoftX (PC labo Windows)")
    print("=" * 60)

    env = os.environ.copy()
    env.setdefault("DXD_WATCH_FOLDER", DXD_WATCH_FOLDER_DEFAUT)
    print(
        "\n  Rôle : ingestion par dépôt de fichiers .dxd et publication\n"
        "         MQTT vers le broker cloud.\n"
        "  Buffer SQLite local activé si le réseau est indisponible.\n"
        f"  Dossier surveillé : {env['DXD_WATCH_FOLDER']}\n"
    )
    chemin_script = os.path.join(BASE, "ingestion_dewesoft_dxd.py")

    print("▶ Démarrage de l'ingestion DeweSoftX...")
    print("  Ctrl+C pour arrêter proprement.\n")

    try:
        subprocess.run([sys.executable, "-u", chemin_script], env=env)
    except KeyboardInterrupt:
        print("\n👋 Ingestion DeweSoftX arrêtée.")
