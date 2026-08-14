"""
Point d'entrée de la plateforme MurMetric — Raspberry Pi, FRD-CODEM.

Lance automatiquement la séquence au démarrage :

1. configure_capteurs.py  (bloquant — attend la fin)
   Scan BLE passif (30 s) pour inventorier les capteurs à portée, puis
   connexion GATT active sur ceux qui ne sont pas encore configurés afin de
   régler l'intervalle de log à sa valeur maximale (économie de pile).
   Les capteurs déjà marqués ``lint_configure: true`` dans capteurs.json
   sont ignorés sans aucune connexion.

2. ingestion_capteurs_bluetooth.py  (bloquant — boucle infinie)
   Ingestion continue : scan BLE passif permanent, décodage des trames
   advertising, publication MQTT (cloud ou SQLite local en cas d'indispo).
   Une tâche asyncio de fond tente périodiquement (défaut : 6 h) de
   reconfigurer les capteurs qui n'auraient pas été présents lors du
   démarrage.

Note architecturale
-------------------
Le bridge MQTT → InfluxDB (bridge_mqtt_to_influx.py) tourne sur le VPS
cloud dans un conteneur Docker dédié (voir docker-compose.yml). Il n'est
pas lancé ici pour éviter les doublons d'écriture dans InfluxDB.

Usage :
    python start.py

    Variables d'environnement reconnues :
        MQTT_BROKER       Adresse du broker MQTT cloud (défaut : localhost)
        MQTT_PORT         Port MQTT                    (défaut : 1883)
        RECONF_INTERVAL   Reconfiguration périodique en secondes (défaut : 21600)
        SQLITE_RETENTION  Rétention buffer local en jours         (défaut : 7)
        SYNC_BATCH_SIZE   Messages par batch de rattrapage         (défaut : 50)
        SYNC_INTERVAL     Intervalle sync SQLite en secondes       (défaut : 30)
"""

import os
import subprocess
import sys

# Encodage UTF-8 pour les accents du résumé de démarrage sous Windows.
sys.stdout.reconfigure(encoding="utf-8")

# Répertoire contenant les scripts (même dossier que ce fichier).
BASE = os.path.dirname(os.path.abspath(__file__))


def lancer(script: str, **kwargs) -> int:
    """Exécuter un script Python en sous-processus bloquant.

    Le sous-processus hérite du stdout/stderr courant (pas de capture) afin
    que sa sortie s'affiche directement dans le terminal. L'option ``-u``
    désactive le buffering Python pour un affichage immédiat.

    Args:
        script: Nom du fichier Python (relatif à BASE).
        **kwargs: Arguments supplémentaires passés à subprocess.run().

    Returns:
        Code de retour du sous-processus (0 = succès).
    """
    chemin = os.path.join(BASE, script)
    resultat = subprocess.run([sys.executable, "-u", chemin], **kwargs)
    return resultat.returncode


def lancer_arriere_plan(script: str) -> subprocess.Popen:
    """Démarrer un script Python en arrière-plan (non bloquant).

    Utilisé pour les services parallèles (bridge MQTT → InfluxDB) qui
    doivent tourner en même temps que l'ingestion.

    Args:
        script: Nom du fichier Python (relatif à BASE).

    Returns:
        Instance Popen du sous-processus, à terminer explicitement.
    """
    chemin = os.path.join(BASE, script)
    return subprocess.Popen([sys.executable, "-u", chemin])


if __name__ == "__main__":
    print("=" * 60)
    print("  MurMetric — Démarrage RPi (capteurs BLE)")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Phase 1 — Configuration GATT des capteurs.
    # ------------------------------------------------------------------
    print("\n▶ Phase 1 : Configuration des capteurs (intervalle log max)...")
    code = lancer("configure_capteurs.py")
    if code != 0:
        # Une erreur de configuration (capteur hors portée, GATT timeout…)
        # n'est pas bloquante : l'ingestion démarre quoi qu'il arrive,
        # et la tâche périodique réessaiera plus tard.
        print(
            f"  Attention : configure_capteurs.py terminé avec code {code} "
            "— la suite démarre quand même."
        )

    # ------------------------------------------------------------------
    # Phase 2 — Ingestion continue (bloquante).
    # ------------------------------------------------------------------
    print("\n▶ Phase 2 : Ingestion continue (scan BLE → décodage → MQTT)...")
    print("  Ctrl+C pour arrêter proprement.\n")
    try:
        lancer("ingestion_capteurs_bluetooth.py")
    except KeyboardInterrupt:
        pass
    finally:
        print("MurMetric arrêté.")
