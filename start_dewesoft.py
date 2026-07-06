"""
Point d'entrée de l'ingestion DeweSoftX — PC labo Windows, FRD-CODEM.

Ce script est destiné à être exécuté sur le PC Windows hébergeant
DeweSoftX. Il lance uniquement ``ingestion_dewesoft.py``, qui acquiert
les mesures du capteur de retrait via DSRemoteConnect et les publie sur
le broker MQTT cloud, avec buffer SQLite local en cas d'indisponibilité
réseau.

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

Usage :
    python start_dewesoft.py

    Variables d'environnement reconnues (cf. ingestion_dewesoft.py) :
        MQTT_BROKER       Adresse du broker MQTT cloud  (défaut : localhost)
        MQTT_PORT         Port MQTT                     (défaut : 1883)
        DSX_HOST          Hôte DeweSoftX NET mode       (défaut : localhost)
        DSX_PORT_DATA     Port données DeweSoftX        (défaut : 8999)
        DSX_PORT_CONTROL  Port contrôle DeweSoftX       (défaut : 8001)
        SQLITE_RETENTION  Rétention buffer local (jours)(défaut : 7)
        SYNC_BATCH_SIZE   Messages par batch de rattrapage (défaut : 50)
        SYNC_INTERVAL     Intervalle sync SQLite (s)    (défaut : 30)
"""

import os
import subprocess
import sys

sys.stdout.reconfigure(encoding="utf-8")

BASE = os.path.dirname(os.path.abspath(__file__))

DLL_NOM = "DSRemoteConnect64.dll"
DLL_CHEMIN_DSX = r"C:\ProgramData\DEWESoft\DSRemoteConnect"


def _verifier_prerequisites() -> bool:
    """Vérifier que la DLL DSRemoteConnect64.dll est présente avant de démarrer.

    Returns:
        True si la DLL est trouvée dans le répertoire du projet, False sinon.
    """
    dll_path = os.path.join(BASE, DLL_NOM)
    if not os.path.exists(dll_path):
        print(
            f"❌ {DLL_NOM} introuvable dans :\n"
            f"   {BASE}\n\n"
            f"   Copiez la DLL depuis l'installation DeweSoftX :\n"
            f"   {DLL_CHEMIN_DSX}\\"
        )
        return False
    return True


if __name__ == "__main__":
    print("=" * 60)
    print("  MurMetric — Ingestion DeweSoftX (PC labo Windows)")
    print("=" * 60)
    print(
        "\n  Rôle : acquisition des mesures de retrait via DeweSoftX\n"
        "         et publication MQTT vers le broker cloud.\n"
        "  Buffer SQLite local activé si le réseau est indisponible.\n"
    )

    if not _verifier_prerequisites():
        sys.exit(1)

    chemin_script = os.path.join(BASE, "ingestion_dewesoft.py")
    print("▶ Démarrage de l'ingestion DeweSoftX...")
    print("  Ctrl+C pour arrêter proprement.\n")

    try:
        subprocess.run([sys.executable, "-u", chemin_script])
    except KeyboardInterrupt:
        print("\n👋 Ingestion DeweSoftX arrêtée.")
