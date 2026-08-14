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

Étiquetage mur/couche/position (registre "capteurs retrait" de la webapp,
source unique depuis le 13/08/2026, cf. logique_projet.md section 32 ;
capteurs_retrait_cache.json local sert de repli hors-ligne seulement) :
chaque canal DeweSoft rencontré est auto-enregistré (entrée vide,
ingestion: false) au premier fichier .dxd qui le contient — aucune donnée
n'est publiée pour un canal tant que l'utilisateur n'a pas complété son
étiquetage et activé l'ingestion depuis la page Capteurs de la webapp. Un
même nom de canal
apparaissant deux fois dans un seul fichier (collision) est signalé en
console ET publié comme point InfluxDB (mesure alertes_ingestion, via le
topic MQTT_TOPIC_ALERTES) plutôt que fusionné silencieusement.

⚠️ HAMPEL_SEUIL_K ci-dessous est un réglage par défaut appliqué une fois à
l'ingestion — il ne correspond PAS encore à un réglage ajustable en direct
depuis l'application (ça demanderait de recalculer le filtre à la demande
côté backend/API à partir de ``valeur`` brute, composant qui n'existe pas
encore dans ce dépôt). En attendant, changer ce seuil impose de retraiter les
fichiers .dxd sources (jamais déplacés ni supprimés — il suffit d'effacer
leur ligne du registre DXD_REGISTRE_FILE pour les rendre à nouveau éligibles)
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
        DXD_WATCH_FOLDER     Dossier surveillé, LECTURE SEULE (obligatoire)
        DXD_REGISTRE_FILE    Registre SQLite des fichiers déjà traités
                             (défaut : <dossier du script>/fichiers_traites.db)
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
import socket
import sqlite3
import statistics
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
import requests

# NumPy accélère le filtre de Hampel et relâche le GIL pendant les calculs
# lourds, ce qui évite d'étouffer le thread réseau de paho (cf.
# _filtrer_hampel_numpy). Facultatif : repli automatique sur la version pure
# Python si la bibliothèque est absente.
try:
    import numpy as np
except ImportError:  # pragma: no cover
    np = None

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

# ---------------------------------------------------------------------------
# Keepalive MQTT (09/08/2026).
#
# Mosquitto déconnecte un client resté muet pendant 1,5 × keepalive. Avec le
# défaut de 60 s, le backfill du 07→09/08/2026 a subi 1 560 déconnexions
# « has exceeded timeout, disconnecting » : chacune faisait basculer
# l'extraction sur le buffer SQLite, qui a fini à 2,6 Go.
#
# La cause exacte du silence du client n'est PAS formellement établie : le
# filtre de Hampel en pur Python monopolisait le CPU (corrigé, cf.
# _filtrer_hampel_numpy), mais une saturation du socket TLS côté broker
# (mosquitto est limité à 250m de CPU sur un nœud partagé) expliquerait tout
# aussi bien l'absence de PINGREQ. Plutôt que de parier sur un diagnostic,
# on élargit franchement la tolérance : 300 s de keepalive laissent 450 s de
# silence avant déconnexion, sans rien coûter (une vraie coupure réseau reste
# détectée immédiatement par l'erreur de socket, pas par le keepalive).
# ---------------------------------------------------------------------------
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "300"))
MQTT_CA_CERT = os.getenv("MQTT_CA_CERT", "")
MQTT_TOPIC_DEWESOFT = os.getenv("MQTT_TOPIC_DEWESOFT", "frd/dewesoft/bruts")
MQTT_TOPIC_ALERTES = os.getenv("MQTT_TOPIC_ALERTES", "frd/dewesoft/alertes")
# Battement de vie pour le monitoring des pipelines côté webapp (section 32,
# 13/08/2026) — cf. logique_projet.md, monitoring_mqtt.py côté backend.
MQTT_TOPIC_HEARTBEAT = os.getenv("MQTT_TOPIC_HEARTBEAT", "frd/monitoring/heartbeat")
HEARTBEAT_INTERVAL_S = float(os.getenv("HEARTBEAT_INTERVAL_S", "300"))
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
#
# Chantier "source unique" (section 32, 13/08/2026) : ce registre n'est plus
# un fichier local géré à la main sur le PC Amiens — la webapp (hébergée sur
# le VPS) en est désormais la source de vérité, ce script interroge son API.
# CAPTEURS_RETRAIT_FILE devient un CACHE local (utilisé si l'API est
# injoignable au démarrage ou lors d'un rafraîchissement — panne réseau,
# webapp temporairement indisponible), jamais la source primaire.
# ---------------------------------------------------------------------------
CAPTEURS_RETRAIT_API_URL = os.getenv("CAPTEURS_API_URL", "http://localhost:8090").rstrip("/") + "/api/capteurs/retrait"
INGESTION_API_KEY = os.getenv("INGESTION_API_KEY", "")
CAPTEURS_RETRAIT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "capteurs_retrait_cache.json"
)
# Le registre ne change pas à chaque fichier .dxd traité : on évite un appel
# HTTP à chaque tour de boucle (POLL_INTERVAL_DXD = 5 s par défaut) en ne
# rafraîchissant réellement que toutes les CAPTEURS_RETRAIT_RAFRAICHISSEMENT_S
# secondes (même logique que l'ancien hot-reload par mtime, mais basée sur le
# temps écoulé puisqu'il n'y a plus de fichier local à surveiller).
CAPTEURS_RETRAIT_RAFRAICHISSEMENT_S = float(os.getenv("CAPTEURS_RETRAIT_RAFRAICHISSEMENT_S", "60"))

# ---------------------------------------------------------------------------
# Configuration de la surveillance du dossier .dxd.
# ---------------------------------------------------------------------------
DXD_WATCH_FOLDER = os.getenv("DXD_WATCH_FOLDER", "")

# ---------------------------------------------------------------------------
# RÈGLE ABSOLUE (09/08/2026) : le dossier surveillé est en LECTURE SEULE.
#
# Jusqu'ici, un fichier traité était DÉPLACÉ vers <watch>/traites (ou
# <watch>/erreurs). Ce déplacement s'est révélé destructeur en production :
# DeweSoftX possède ce dossier et continue de référencer les enregistrements
# qu'il contient. Voir disparaître un .dxd déclenche chez lui une
# régénération de segments de récupération « <nom>.lostNN.dxd » dans le même
# dossier. Mesuré sur le backfill du 07→09/08/2026 : 1 226 fichiers .lost
# créés pendant le run, corpus passé de 6,07 à 11,07 Gio, 352 fichiers
# traités deux fois — le backfill ne pouvait plus converger, le dossier se
# remplissait aussi vite qu'il se vidait.
#
# CORRECTIF : plus AUCUNE écriture, aucun déplacement, aucun renommage,
# aucune suppression sous DXD_WATCH_FOLDER — pas même vers un sous-dossier.
# L'état « déjà traité » vit désormais dans un registre SQLite EXTERNE au
# dossier surveillé (DXD_REGISTRE_FILE ci-dessous). La boucle de surveillance
# ignore tout fichier déjà inscrit au registre, ce qui reproduit exactement
# l'ancien comportement (un fichier déplacé hors du dossier n'était plus vu)
# sans toucher au disque de DeweSoftX.
#
# Reprise après redémarrage : c'est le REGISTRE, et non le contenu du
# dossier, qui fait foi. Le registre est relu au démarrage.
#
# Retraitement d'un fichier (ex. changement de HAMPEL_SEUIL_K, cf. docstring
# du module) : supprimer sa ligne du registre suffit à le rendre à nouveau
# éligible au prochain scan. Aucun outil dédié n'est nécessaire.
#   sqlite3 fichiers_traites.db "DELETE FROM fichiers_traites WHERE nom_fichier='X.dxd'"
#
# DXD_PROCESSED_FOLDER / DXD_ERROR_FOLDER ont été SUPPRIMÉS : ils n'ont plus
# d'objet, et les conserver inviterait à réintroduire un déplacement.
# ---------------------------------------------------------------------------
DXD_REGISTRE_FILE = os.getenv(
    "DXD_REGISTRE_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "fichiers_traites.db"),
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

# ---------------------------------------------------------------------------
# Débit de vidange du buffer SQLite (09/08/2026).
#
# Anciennes valeurs : 50 messages toutes les 30 s, avec en plus un
# time.sleep(0.05) ET un wait_for_publish() synchrone PAR message. Débit
# plafond réel : ~1,7 message/s ≈ 1 000 échantillons/s — face à une
# extraction qui en produit ~30 000/s. Mathématiquement, le buffer ne
# pouvait JAMAIS rattraper son retard : une fois la bascule SQLite amorcée,
# il ne faisait que grossir. Constaté le 09/08/2026 : 2,6 Go de buffer,
# 101 592 messages en attente, puis des « database is locked » qui ont fait
# échouer 349 fichiers.
#
# Nouvelles valeurs : 500 messages toutes les 5 s, publiés en rafale puis
# acquittés en bloc sous échéance globale (cf. tache_sync_sqlite) — soit
# ~100 messages/s ≈ 60 000 échantillons/s, largement au-dessus du débit
# d'extraction (~30 000/s). Le buffer redevient ce qu'il doit être : un
# tampon transitoire qui se vide dès que le lien MQTT revient.
#
# Pourquoi 500 et non 5 000 : les lots font ~17 ko, donc un fetchall() de
# 5 000 lignes charge 85 Mo en mémoire — et le fait en tenant _verrou_sqlite,
# ce qui bloque d'autant le thread d'extraction qui veut y écrire. 500 lignes
# ≈ 8,5 Mo, sans effet mesurable sur le débit de vidange.
# ---------------------------------------------------------------------------
SYNC_BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "500"))
SYNC_INTERVAL_SECONDES = int(os.getenv("SYNC_INTERVAL", "5"))

_mqtt_connecte: bool = False

# Horodatage de démarrage du script — publié dans le battement de vie
# (cf. envoyer_heartbeat) pour que la page Monitoring de la webapp puisse
# afficher depuis quand le process tourne sans interruption.
_DEMARRAGE = datetime.now()

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

# ---------------------------------------------------------------------------
# UNE SEULE connexion SQLite, partagée (09/08/2026).
#
# Chaque fonction ouvrait auparavant sa propre connexion — donc un
# sqlite3.connect() PAR MESSAGE bufferisé, et un autre par suppression lors
# de la vidange. Sur un buffer devenu volumineux (2,6 Go le 09/08/2026),
# ouvrir/fermer sans cesse un fichier de cette taille, chacun avec son propre
# verrou, a fini par produire des « database is locked » qui ont fait échouer
# 349 fichiers. Une connexion unique en mode WAL (lecteurs et écrivain
# concurrents) avec un busy_timeout franc supprime les deux problèmes.
#
# check_same_thread=False : la connexion est utilisée par le thread
# d'extraction ET par le thread de synchronisation ; tous les accès passent
# par _verrou_sqlite.
# ---------------------------------------------------------------------------
_conn_sqlite: sqlite3.Connection | None = None
_verrou_sqlite = threading.Lock()


def initialiser_sqlite() -> None:
    """Ouvrir la connexion partagée et créer la table de buffer."""
    global _conn_sqlite
    _conn_sqlite = sqlite3.connect(
        SQLITE_BUFFER_FILE, check_same_thread=False, timeout=60.0)
    with _verrou_sqlite:
        # WAL : un écrivain n'empêche plus les lecteurs (et inversement).
        _conn_sqlite.execute("PRAGMA journal_mode=WAL")
        # 60 s d'attente d'un verrou plutôt qu'un échec immédiat.
        _conn_sqlite.execute("PRAGMA busy_timeout=60000")
        # NORMAL : pas de fsync à chaque transaction ; en WAL, la durabilité
        # reste suffisante pour un buffer de secours.
        _conn_sqlite.execute("PRAGMA synchronous=NORMAL")
        _conn_sqlite.execute("""
            CREATE TABLE IF NOT EXISTS buffer_mqtt (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                topic       TEXT    NOT NULL,
                payload     TEXT    NOT NULL,
                horodatage  TEXT    NOT NULL,
                tente_le    TEXT    DEFAULT NULL
            )
        """)
        _conn_sqlite.commit()


def stocker_localement(topic: str, payload_json: str) -> None:
    """Persister un message MQTT dans le buffer SQLite local."""
    horodatage = datetime.now().isoformat()
    with _verrou_sqlite:
        _conn_sqlite.execute(
            "INSERT INTO buffer_mqtt (topic, payload, horodatage) VALUES (?, ?, ?)",
            (topic, payload_json, horodatage),
        )
        _conn_sqlite.commit()


def purger_buffer_expire() -> int:
    """Supprimer les messages plus anciens que SQLITE_RETENTION_JOURS jours.

    Returns:
        Nombre de messages supprimés.
    """
    limite = (datetime.now() - timedelta(days=SQLITE_RETENTION_JOURS)).isoformat()
    with _verrou_sqlite:
        cursor = _conn_sqlite.execute(
            "DELETE FROM buffer_mqtt WHERE horodatage < ?", (limite,))
        _conn_sqlite.commit()
        return cursor.rowcount


def compter_messages_en_attente() -> int:
    """Retourner le nombre de messages SQLite non encore envoyés."""
    with _verrou_sqlite:
        (count,) = _conn_sqlite.execute("SELECT COUNT(*) FROM buffer_mqtt").fetchone()
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
# Registre d'étiquetage des capteurs de retrait — API webapp (source unique,
# section 32, 13/08/2026), avec repli sur un cache local si l'API est
# injoignable (réseau, webapp temporairement indisponible). Symétrique du
# registre BLE (capteurs.json / ingestion_capteurs_bluetooth.py) mais sans
# adresse MAC : la clé est le nom de canal DeweSoft (ex. "HA1"), fixé une
# fois pour toutes par le câblage du rig — pas de découverte automatique
# équivalente au BLE, cf. logique_projet.md section 19.
# ===========================================================================

_fichier_retrait_lock = threading.Lock()
_capteurs_retrait_prochain_rafraichissement: float = 0.0
CAPTEURS_RETRAIT_CONNUS: dict = {}
# Dernier résultat connu de _recuperer_registre_retrait_distant() — exposé
# dans le battement de vie (cf. envoyer_heartbeat) pour distinguer "l'API a
# répondu, tout va bien" de "on tourne sur le cache local depuis un moment".
_dernier_registre_api_ok: bool | None = None


def _recuperer_registre_retrait_distant() -> dict | None:
    """Récupérer capteurs_retrait.json depuis l'API webapp.

    Returns:
        Le mapping canal → infos, ou None si l'API est injoignable, en
        timeout, répond une erreur HTTP ou un JSON invalide — dans tous ces
        cas l'appelant doit conserver le registre en mémoire tel quel (ou se
        rabattre sur le cache local) plutôt que le vider.
    """
    global _dernier_registre_api_ok
    try:
        reponse = requests.get(CAPTEURS_RETRAIT_API_URL, timeout=10)
        reponse.raise_for_status()
        donnees = reponse.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"⚠️  API capteurs_retrait injoignable ({exc}).")
        _dernier_registre_api_ok = False
        return None
    _dernier_registre_api_ok = True
    return {cle: infos for cle, infos in donnees.items() if not cle.startswith("_")}


def _lire_cache_retrait_local() -> dict | None:
    try:
        with open(CAPTEURS_RETRAIT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _ecrire_cache_retrait_local(donnees: dict) -> None:
    try:
        with open(CAPTEURS_RETRAIT_FILE, "w", encoding="utf-8") as f:
            json.dump(donnees, f, indent=2, ensure_ascii=False)
    except OSError as exc:
        print(f"⚠️  Écriture du cache {CAPTEURS_RETRAIT_FILE} impossible ({exc}) — ignoré.")


def _rafraichir_capteurs_retrait_connus() -> None:
    """Récupérer le registre distant et, seulement en cas d'échec, retomber
    sur le cache local le plus récent. Met à jour le cache local à chaque
    récupération distante réussie."""
    global CAPTEURS_RETRAIT_CONNUS
    nouveau = _recuperer_registre_retrait_distant()
    if nouveau is not None:
        CAPTEURS_RETRAIT_CONNUS = nouveau
        _ecrire_cache_retrait_local(nouveau)
        return
    nouveau = _lire_cache_retrait_local()
    if nouveau is not None:
        print(f"↩️  Registre capteurs_retrait repris du cache local ({CAPTEURS_RETRAIT_FILE}).")
        CAPTEURS_RETRAIT_CONNUS = nouveau


def charger_capteurs_retrait_connus() -> None:
    """Charger le registre capteurs_retrait en mémoire au démarrage du script."""
    global _capteurs_retrait_prochain_rafraichissement
    with _fichier_retrait_lock:
        _rafraichir_capteurs_retrait_connus()
        _capteurs_retrait_prochain_rafraichissement = time.monotonic() + CAPTEURS_RETRAIT_RAFRAICHISSEMENT_S


def verifier_et_recharger_capteurs_retrait() -> None:
    """Rafraîchir le registre capteurs_retrait depuis l'API, au rythme de
    CAPTEURS_RETRAIT_RAFRAICHISSEMENT_S (pas à chaque tour de boucle — le
    registre ne change pas à chaque fichier .dxd traité)."""
    global _capteurs_retrait_prochain_rafraichissement
    if time.monotonic() < _capteurs_retrait_prochain_rafraichissement:
        return
    with _fichier_retrait_lock:
        avant = len(CAPTEURS_RETRAIT_CONNUS)
        _rafraichir_capteurs_retrait_connus()
        if len(CAPTEURS_RETRAIT_CONNUS) != avant:
            print(f"🔄 Registre capteurs_retrait rechargé ({len(CAPTEURS_RETRAIT_CONNUS)} canal(aux) connus)")
        _capteurs_retrait_prochain_rafraichissement = time.monotonic() + CAPTEURS_RETRAIT_RAFRAICHISSEMENT_S


def enregistrer_canal_si_inconnu(canal_nom: str) -> None:
    """Déclarer un nouveau canal auprès de l'API webapp avec ingestion: false.

    Aucun canal n'est donc ingéré silencieusement : sa première lecture crée
    une entrée vide à étiqueter depuis la webapp, exactement comme une MAC
    BLE inconnue dans capteurs.json. Si l'API est injoignable, l'entrée reste
    seulement en mémoire pour cette exécution (retentée au prochain
    redémarrage) — aucune mesure n'est perdue, elle reste simplement non
    publiée tant que le canal n'est pas étiqueté et activé.
    """
    global CAPTEURS_RETRAIT_CONNUS

    if canal_nom in CAPTEURS_RETRAIT_CONNUS:
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

    try:
        reponse = requests.post(
            CAPTEURS_RETRAIT_API_URL + "/enregistrer",
            json={"canal": canal_nom},
            headers={"X-Ingestion-Key": INGESTION_API_KEY},
            timeout=10,
        )
        reponse.raise_for_status()
        entree = reponse.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"⚠️  Enregistrement distant du canal {canal_nom} impossible ({exc}) — retenté au prochain démarrage.")

    CAPTEURS_RETRAIT_CONNUS[canal_nom] = entree
    print(
        f"📝 Nouveau canal de retrait enregistré : {canal_nom} "
        "— définissez ingestion=true depuis la page Capteurs de la webapp pour l'activer"
    )


# ===========================================================================
# Battement de vie — monitoring des pipelines côté webapp (section 32,
# 13/08/2026). Publié sur le même canal MQTT que les mesures (buffer SQLite
# en secours si le cloud est injoignable) : un battement perdu ou en retard
# n'a aucune conséquence sur les données réelles, contrairement à un point
# de mesure.
# ===========================================================================

_heartbeat_prochain_envoi: float = 0.0


def envoyer_heartbeat_si_du() -> None:
    global _heartbeat_prochain_envoi
    if time.monotonic() < _heartbeat_prochain_envoi:
        return
    _heartbeat_prochain_envoi = time.monotonic() + HEARTBEAT_INTERVAL_S
    payload = {
        "pipeline": "retrait",
        "machine": socket.gethostname(),
        "demarre_le": _DEMARRAGE.isoformat(),
        "mqtt_connecte": _mqtt_connecte,
        "buffer_sqlite_en_attente": compter_messages_en_attente(),
        "registre_api_ok": _dernier_registre_api_ok,
        "nb_capteurs_connus": len(CAPTEURS_RETRAIT_CONNUS),
        "nb_points_publies": _nb_publies,
        "nb_points_bufferises": _nb_bufferises,
    }
    publier_ou_stocker(MQTT_TOPIC_HEARTBEAT, payload)


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

        with _verrou_sqlite:
            rows = _conn_sqlite.execute(
                "SELECT id, topic, payload FROM buffer_mqtt ORDER BY horodatage ASC LIMIT ?",
                (SYNC_BATCH_SIZE,),
            ).fetchall()

        # Publication EN RAFALE puis acquittement en bloc, sous ÉCHÉANCE
        # GLOBALE (09/08/2026).
        #
        # L'ancienne boucle attendait l'acquittement de chaque message
        # (wait_for_publish) et faisait un time.sleep(0.05) entre deux, soit
        # ~20 messages/s au mieux : le buffer ne se vidait jamais.
        #
        # Attention au piège corrigé le même jour : attendre DRAIN_TIMEOUT
        # PAR message reste catastrophique si le broker cesse d'acquitter —
        # 200 messages × 120 s = 6,6 h de thread de sync bloqué, buffer figé
        # (observé en conditions réelles). L'attente est donc bornée par une
        # échéance unique pour tout le lot : passé ce délai, on abandonne le
        # reste du lot, on efface ce qui a été confirmé, et on rendra la main
        # au cycle suivant. Rien n'est perdu : ce qui n'est pas confirmé
        # reste en base.
        echeance = time.monotonic() + DRAIN_TIMEOUT
        en_vol: list[tuple[int, object]] = []
        for row_id, topic, payload in rows:
            if not _mqtt_connecte:
                print("⚠️  Déconnexion pendant la sync — arrêt du batch.")
                break
            if time.monotonic() > echeance:
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
                en_vol.append((row_id, result))
            except Exception as exc:
                _liberer_place()
                print(f"⚠️  Erreur sync message {row_id} : {exc}")
                erreurs += 1

        # N'effacer du buffer que ce que le broker a réellement acquitté.
        ids_confirmes = []
        for row_id, result in en_vol:
            reste = echeance - time.monotonic()
            try:
                if reste > 0:
                    result.wait_for_publish(timeout=reste)
                if result.is_published():
                    ids_confirmes.append((row_id,))
                else:
                    erreurs += 1
            except Exception:
                erreurs += 1

        if ids_confirmes:
            with _verrou_sqlite:
                _conn_sqlite.executemany(
                    "DELETE FROM buffer_mqtt WHERE id = ?", ids_confirmes)
                _conn_sqlite.commit()
            envoyes = len(ids_confirmes)

        restants = compter_messages_en_attente()
        print(f"✅ Sync batch terminé : {envoyes} envoyés, {erreurs} erreurs, {restants} restants.")

        # S'il reste des messages, se re-déclencher immédiatement.
        if restants > 0 and _mqtt_connecte:
            _sync_event.set()


# ===========================================================================
# Détection de fichier stable (copie/écriture terminée).
# ===========================================================================

# Prototypes Win32 pour la sonde de verrou. restype/argtypes sont
# OBLIGATOIRES : sans eux, ctypes suppose un retour c_int et TRONQUE le
# handle 64 bits, si bien que la comparaison à INVALID_HANDLE_VALUE échoue
# toujours et que la sonde répond « jamais verrouillé » (bug attrapé par
# test_registre.py avant déploiement).
_GENERIC_READ = 0x80000000
_OPEN_EXISTING = 3
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

_CreateFileW = ctypes.windll.kernel32.CreateFileW
_CreateFileW.restype = ctypes.c_void_p
_CreateFileW.argtypes = [
    ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p,
    ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p,
]
_CloseHandle = ctypes.windll.kernel32.CloseHandle
_CloseHandle.restype = ctypes.c_int
_CloseHandle.argtypes = [ctypes.c_void_p]

def fichier_est_verrouille(chemin: str) -> bool:
    """Détecter si un processus tient encore le fichier ouvert (écriture en cours).

    Sonde STRICTEMENT en lecture seule : on demande à Windows d'ouvrir le
    fichier en lecture avec un partage NUL (dwShareMode=0). Si un autre
    processus — typiquement DeweSoftX en train d'enregistrer — le tient
    ouvert, l'appel échoue avec ERROR_SHARING_VIOLATION.

    L'ancienne astuce ``os.rename(chemin, chemin)`` faisait le même travail
    mais était une opération d'ÉCRITURE sur le dossier de DeweSoftX : elle
    est proscrite depuis le 09/08/2026 (cf. DXD_REGISTRE_FILE — le moindre
    remaniement sous DXD_WATCH_FOLDER déclenche la régénération de fichiers
    .lostNN par DeweSoftX). Ouvrir en lecture ne modifie ni le contenu, ni le
    nom, ni la taille du fichier.
    """
    handle = _CreateFileW(
        chemin,
        _GENERIC_READ,
        0,                  # aucun partage : détecte tout autre ouvreur
        None,
        _OPEN_EXISTING,
        0,
        None,
    )
    if handle is None or handle == _INVALID_HANDLE_VALUE:
        return True
    _CloseHandle(handle)
    return False


def attendre_fichier_stable(chemin: str) -> bool:
    """Attendre que la taille du fichier soit stable et qu'il ne soit plus verrouillé.

    Peut bloquer très longtemps (jusqu'à ~12h) sur le fichier DeweSoft en
    cours d'écriture — sa taille ne se stabilise qu'à la rotation suivante.
    Pendant ce temps, la boucle extérieure de boucle_surveillance() (où
    vivent verifier_et_recharger_capteurs_retrait() et
    envoyer_heartbeat_si_du()) n'est jamais réatteinte : sans les appeler
    ici aussi, le registre capteurs et le battement de vie resteraient
    figés pendant tout ce blocage (trouvé le 13/08/2026 — le tout premier
    battement envoyé au démarrage, avec mqtt_connecte à sa valeur du split
    second avant la fin de la poignée de main, restait affiché indéfiniment
    côté webapp alors que le script tournait normalement).

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
        verifier_et_recharger_capteurs_retrait()
        envoyer_heartbeat_si_du()
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


def _filtrer_hampel_python(
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


def _filtrer_hampel_numpy(
    valeurs: list[float],
    demi_fenetre: int = HAMPEL_FENETRE,
    seuil_k: float = HAMPEL_SEUIL_K,
) -> tuple[list[float], set[int]]:
    """Version vectorisée NumPy de :func:`_filtrer_hampel_python`.

    Résultat strictement identique à la version pure Python (vérifié par
    comparaison exhaustive, cf. ``test_hampel_equivalence.py``) — seule la
    vitesse change.

    POURQUOI (09/08/2026) : la version pure Python enchaîne ~2 appels à
    ``statistics.median`` par échantillon, soit ~1,7 million d'appels pour un
    canal de 432 000 points. Elle monopolise le GIL pendant des dizaines de
    secondes d'affilée, ce qui empêche le thread réseau de paho d'émettre ses
    PINGREQ : le broker déclare alors le client muet et le déconnecte
    (« has exceeded timeout, disconnecting »). Mesuré sur le backfill du
    07→09/08/2026 : 1 560 déconnexions MQTT, bascule massive vers le buffer
    SQLite, puis 349 fichiers en échec sur « database is locked ».

    NumPy effectue ces médianes dans ses boucles C, qui relâchent le GIL :
    le thread réseau reprend la main régulièrement et le keepalive passe.

    Les fenêtres sont TRONQUÉES aux bords (comme la version de référence) :
    les ``demi_fenetre`` premiers et derniers indices n'ont pas de fenêtre
    complète et sont donc calculés un par un, à l'identique. Le cœur du
    signal, lui, est traité par blocs pour borner la mémoire.
    """
    n = len(valeurs)
    v = np.asarray(valeurs, dtype=np.float64)
    filtrees = v.copy()
    aberrants: set[int] = set()
    facteur_mad = 1.4826
    largeur = 2 * demi_fenetre + 1

    # --- Cœur du signal : fenêtres complètes, vectorisées par blocs --------
    if n >= largeur:
        vues = np.lib.stride_tricks.sliding_window_view(v, largeur)
        # 65 536 fenêtres par bloc ≈ 11 Mo par tableau temporaire : les
        # fichiers .lost peuvent dépasser 3 millions d'échantillons, tout
        # traiter d'un coup demanderait plusieurs Go.
        bloc = 65_536
        for debut in range(0, vues.shape[0], bloc):
            fen = vues[debut:debut + bloc]
            mediane = np.median(fen, axis=1)
            mad = np.median(np.abs(fen - mediane[:, None]), axis=1) * facteur_mad

            diffs = np.diff(fen, axis=1)
            mediane_diff = np.median(diffs, axis=1)
            mad_diff = np.median(
                np.abs(diffs - mediane_diff[:, None]), axis=1) * facteur_mad

            mad_effectif = np.maximum(mad, mad_diff)
            centre = v[debut + demi_fenetre:debut + demi_fenetre + fen.shape[0]]
            masque = (mad_effectif > 0) & (
                np.abs(centre - mediane) > seuil_k * mad_effectif)

            positions = np.nonzero(masque)[0]
            if positions.size:
                indices = positions + debut + demi_fenetre
                filtrees[indices] = mediane[positions]
                aberrants.update(int(x) for x in indices)

    # --- Bords : fenêtres tronquées, calculées exactement comme la référence
    bords = set(range(0, min(demi_fenetre, n)))
    bords |= set(range(max(0, n - demi_fenetre), n))
    for i in sorted(bords):
        lo = max(0, i - demi_fenetre)
        hi = min(n, i + demi_fenetre + 1)
        fenetre = valeurs[lo:hi]
        mediane = statistics.median(fenetre)
        mad = statistics.median([abs(x - mediane) for x in fenetre]) * facteur_mad

        diffs_fenetre = [fenetre[k] - fenetre[k - 1] for k in range(1, len(fenetre))]
        mad_diff = 0.0
        if diffs_fenetre:
            mediane_diff = statistics.median(diffs_fenetre)
            mad_diff = statistics.median(
                [abs(d - mediane_diff) for d in diffs_fenetre]) * facteur_mad

        mad_effectif = max(mad, mad_diff)
        if mad_effectif > 0 and abs(valeurs[i] - mediane) > seuil_k * mad_effectif:
            aberrants.add(i)
            filtrees[i] = mediane

    # tolist() : indispensable, json.dumps() ne sait pas sérialiser np.float64.
    return filtrees.tolist(), aberrants


def filtrer_hampel(
    valeurs: list[float],
    demi_fenetre: int = HAMPEL_FENETRE,
    seuil_k: float = HAMPEL_SEUIL_K,
) -> tuple[list[float], set[int]]:
    """Filtre de Hampel — vectorisé si NumPy est disponible, sinon pur Python.

    Les deux implémentations produisent le même résultat ; NumPy est
    nettement plus rapide et, surtout, relâche le GIL (cf.
    :func:`_filtrer_hampel_numpy`).
    """
    if np is not None:
        return _filtrer_hampel_numpy(valeurs, demi_fenetre, seuil_k)
    return _filtrer_hampel_python(valeurs, demi_fenetre, seuil_k)


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
        # DWICloseDataFile doit être appelé même quand DWIOpenDataFile ÉCHOUE.
        # Sur un .dxd corrompu (« Error 4: File is corrupted or has invalid
        # format »), le SDK laisse malgré tout un handle Windows ouvert sur le
        # fichier, et DWIDestroyReader ne le libère PAS. Le déplacement vers
        # erreurs/ échouait alors avec WinError 32 (« fichier utilisé par un
        # autre processus »), et cette seconde exception — levée depuis le
        # gestionnaire d'erreur de boucle_surveillance() — tuait le processus.
        # Constaté le 07/08/2026 : backfill mort au 10e fichier sur 949.
        try:
            check_error(
                lib, lib.DWIOpenDataFile(reader, c_filename, ctypes.byref(file_info))
            )
        except Exception:
            try:
                lib.DWICloseDataFile(reader)
            except Exception:
                pass
            raise

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
                        "(page Capteurs de la webapp) — canal ignoré."
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

def initialiser_registre() -> None:
    """Créer le registre des fichiers déjà traités (hors dossier surveillé).

    Remplace l'ancien déplacement vers traites/ et erreurs/ : le dossier de
    DeweSoftX ne doit plus être modifié du tout (cf. DXD_REGISTRE_FILE).
    """
    with sqlite3.connect(DXD_REGISTRE_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fichiers_traites (
                nom_fichier TEXT PRIMARY KEY,
                statut      TEXT    NOT NULL,
                horodatage  TEXT    NOT NULL,
                nb_canaux   INTEGER,
                nb_mesures  INTEGER,
                raison      TEXT
            )
        """)
        conn.commit()


def charger_registre() -> set[str]:
    """Charger en mémoire les noms de fichiers déjà traités ou en erreur.

    C'est ce jeu — et non le contenu du dossier — qui fait foi pour savoir
    ce qui reste à faire, y compris après un redémarrage du processus.
    """
    with sqlite3.connect(DXD_REGISTRE_FILE) as conn:
        return {nom for (nom,) in conn.execute(
            "SELECT nom_fichier FROM fichiers_traites")}


def enregistrer_fichier(
    nom: str,
    statut: str,
    nb_canaux: int | None = None,
    nb_mesures: int | None = None,
    raison: str | None = None,
) -> None:
    """Inscrire un fichier au registre (statut ``traite`` ou ``erreur``).

    Supprimer cette ligne suffit à rendre le fichier éligible à un
    retraitement au prochain scan (ex. après changement de HAMPEL_SEUIL_K).
    """
    with sqlite3.connect(DXD_REGISTRE_FILE) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO fichiers_traites "
            "(nom_fichier, statut, horodatage, nb_canaux, nb_mesures, raison) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (nom, statut, datetime.now().isoformat(), nb_canaux, nb_mesures, raison),
        )
        conn.commit()


# ===========================================================================
# Boucle de surveillance du dossier.
# ===========================================================================

def boucle_surveillance() -> None:
    """Scanner le dossier surveillé en continu et traiter chaque .dxd détecté."""
    print(f"\n👀 Surveillance du dossier : {DXD_WATCH_FOLDER}")
    print(f"   Poll toutes les {POLL_INTERVAL_DXD} s. Arrêt : Ctrl+C\n")

    deja_signales: set[str] = set()

    # Source de vérité du « déjà fait » : le registre externe, pas le dossier
    # (qu'on ne modifie plus du tout). Relu ici au démarrage pour que la
    # reprise après redémarrage ne retraite rien.
    deja_traites: set[str] = charger_registre()
    print(f"📒 Registre : {len(deja_traites)} fichier(s) déjà traité(s) — ignorés.\n")

    while True:
        verifier_et_recharger_capteurs_retrait()
        envoyer_heartbeat_si_du()

        try:
            noms_fichiers = os.listdir(DXD_WATCH_FOLDER)
        except OSError as exc:
            print(f"❌ Dossier surveillé inaccessible ({exc}) — nouvelle tentative...")
            time.sleep(POLL_INTERVAL_DXD)
            continue

        for nom in noms_fichiers:
            if not nom.lower().endswith(".dxd"):
                continue
            # Déjà traité (ou déjà en erreur définitive) : on l'ignore, sans
            # toucher au fichier — il reste en place pour DeweSoftX.
            if nom in deja_traites:
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
                # forcément acquitté par le broker. Inscrire le fichier au
                # registre à cet instant reviendrait à le déclarer traité
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
                        f"après {DRAIN_TIMEOUT}s — NON inscrit au registre, "
                        "sera retenté au prochain passage."
                    )
                    continue
                print(
                    f"✅ {nom} : {nb_canaux} canal(aux), {nb_mesures} mesure(s) "
                    f"en {duree:.0f}s ({nb_mesures / max(duree, 1e-9):.0f} mesures/s) — "
                    f"{_nb_publies} publiées MQTT, {_nb_bufferises} bufferisées SQLite."
                )
                # Le fichier reste EXACTEMENT où il est ; seul le registre bouge.
                enregistrer_fichier(nom, "traite", nb_canaux, nb_mesures)
                deja_traites.add(nom)
            except Exception as exc:
                print(f"❌ Échec extraction {nom} : {exc}")
                enregistrer_fichier(nom, "erreur", raison=str(exc)[:500])
                deja_traites.add(nom)

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

    # Aucun os.makedirs() ici : le dossier surveillé appartient à DeweSoftX et
    # doit rester strictement intact (cf. DXD_REGISTRE_FILE). On se contente
    # de vérifier qu'il existe.
    if not os.path.isdir(DXD_WATCH_FOLDER):
        print(f"❌ Dossier surveillé introuvable : {DXD_WATCH_FOLDER}")
        sys.exit(1)
    print(f"  Dossier surveillé (LECTURE SEULE) : {DXD_WATCH_FOLDER}")
    print(f"  Registre des fichiers traités     : {DXD_REGISTRE_FILE}")

    initialiser_sqlite()
    initialiser_registre()
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
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
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
