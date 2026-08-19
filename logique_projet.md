# MurMetric — Logique d'ingestion capteurs BLE

Plateforme de monitoring métrologique des parois biosourcées — FRD-CODEM.
Ce document décrit l'architecture complète du système MurMetric : ingestion
des capteurs BLE (température/humidité) et DeweSoftX (retrait), pipeline
cloud Kafka → InfluxDB, résilience SQLite locale, et déploiement SaaS
via Docker et Kubernetes.

## Vue d'ensemble du pipeline

```
RPi Amiens                         PC labo Windows (Amiens)
─────────────────────────────      ──────────────────────────────
python start.py                    python start_dewesoft.py
   │                                  │
   ├─► configure_capteurs.py          └─► ingestion_dewesoft.py
   │      Scan 30 s → GATT                   DeweSoftX NET (1 msg/s)
   │                                         SQLite local si VPS down
   └─► ingestion_capteurs_bluetooth.py        │
          Scan BLE passif permanent           │
          Filtre + Décode + Enrichit          │
          SQLite local si VPS down            │
          │                                  │
          ▼                                  ▼
       ☁️  Cloud VPS (docker-compose / Kubernetes)
       ──────────────────────────────────────────────────────
       Mosquitto (MQTT broker, port 1883)
          │
          ▼
       bridge_mqtt_to_kafka.py
          │  Transfère les messages bruts sans transformation
          ▼
       Kafka (KRaft, rétention 7 jours)
       ┌────────────────────────────────────┐
       │  murmetric.frd.capteurs.bruts      │
       │  murmetric.frd.capteurs.registre   │
       │  murmetric.frd.dewesoft.bruts      │
       └────────────────────────────────────┘
          │
          ▼
       kafka_consumer_influx.py  (batch async, 500 pts / 1 s)
          │
          ▼
       InfluxDB → Grafana
```

**Résilience sur deux segments :**
- **SQLite** (RPi/PC) : protège le trajet terrain → VPS (réseau instable)
- **Kafka** (VPS) : protège le trajet Mosquitto → InfluxDB (panne interne VPS)

## 1. Infrastructure

### Machines de terrain

| Machine | OS | Scripts lancés | Dépendances |
|---|---|---|---|
| **Raspberry Pi** (Amiens) | Linux | `start.py` → configure + BLE ingestion | bleak, paho-mqtt |
| **PC labo Windows** (Amiens) | Windows | `start_dewesoft.py` → DeweSoft ingestion | paho-mqtt, DSRemoteConnect64.dll |

Chaque machine possède son propre `murmetric_buffer.db` (SQLite) et son propre `.venv` :
- RPi : `pip install -r requirements-rpi.txt`
- PC Windows : `pip install -r requirements-windows.txt`

### VPS Cloud — 5 services Docker

Le VPS fait tourner l'intégralité de la stack cloud via `docker-compose.yml` :

| Service | Image | Rôle |
|---|---|---|
| **mosquitto** | eclipse-mosquitto:2.0 | Broker MQTT, reçoit les publications terrain |
| **kafka** | bitnami/kafka:3.7 | Bus de messages, découple producteurs/consommateurs |
| **influxdb** | influxdb:2.7 | Base time-series (org `FRD_CODEM`, bucket `Test_Capteurs`) |
| **bridge** | murmetric-bridge | `bridge_mqtt_to_kafka.py` — MQTT → Kafka |
| **kafka-consumer** | murmetric-kafka-consumer | `kafka_consumer_influx.py` — Kafka → InfluxDB (batch async) |

Démarrage : `docker compose up -d`

### Kubernetes (déploiement SaaS)

Les manifests dans `k8s/` permettent de déployer la stack sur un cluster Kubernetes
(k3s recommandé pour un VPS unique) :

```
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets.yaml        # copié depuis secrets.yaml.template
kubectl apply -f k8s/mosquitto/
kubectl apply -f k8s/kafka/
kubectl apply -f k8s/influxdb/
kubectl apply -f k8s/bridge-mqtt-kafka/
kubectl apply -f k8s/kafka-consumer-influx/
kubectl apply -f k8s/grafana/
```

**Prérequis pour l'autoscaling (`k8s/kafka-consumer-influx/hpa.yaml`) :** le cluster
doit avoir `metrics-server` installé et fonctionnel (k3s l'inclut par défaut).
Vérifier avec `kubectl top pods -n murmetric` — si la commande échoue, l'HPA ne
pourra jamais scaler (il restera bloqué à `minReplicas`).

**Ce qui scale automatiquement, et ce qui ne scale pas :**

| Composant | Scaling auto ? | Pourquoi |
|---|---|---|
| `kafka-consumer-influx` | ✅ HPA (1 à 6 replicas, CPU 70%) | Consumer group Kafka natif — répartition sans doublon |
| `bridge-mqtt-kafka` | ❌ Fixé à 1 replica | Chaque replica s'abonnerait aux mêmes topics MQTT et republierait chaque message en double — nécessiterait des "shared subscriptions" MQTT v5, non implémentées |
| `mosquitto`, `kafka`, `influxdb` | ❌ Instance unique | Composants stateful non clusterisés dans cette configuration (InfluxDB OSS ne supporte pas le clustering horizontal ; Kafka et Mosquitto ici sont configurés en nœud unique) |
| `grafana` | ❌ 1 replica | Stocke son état (dashboards, sessions) en SQLite local — plusieurs replicas auraient chacun leur propre état non partagé |

Si le volume de clients dépasse ce que `kafka-consumer-influx` peut absorber en
scalant seul, la vraie limite sera Kafka/InfluxDB/Mosquitto en nœud unique — un
sujet à traiter séparément (cluster Kafka multi-broker, InfluxDB Cloud/Enterprise,
ou migration vers une autre base time-series).

Le consommateur Kafka est scalable horizontalement :
`kubectl scale deployment kafka-consumer-influx --replicas=3`

## 2. Filtrage des capteurs (scalable à 200+ capteurs)

Le filtrage s'effectue en **trois niveaux successifs** dans `callback()` :

1. **Company ID Bluetooth SIG** (`BLUEMAESTRO_COMPANY_ID = 0x0133`) : seules
   les annonces BLE contenant ce manufacturer ID sont traitées. Élimine tout
   le bruit BLE ambiant (téléphones, objets tiers) sans liste de MAC à maintenir.
2. **Octet "version" du protocole** (`VERSIONS_CONNUES = {13, 23, 27, 41, 42, 43}`) :
   rejette les trames Blue Maestro dont le format n'est pas reconnu.
3. **Champ `ingestion` dans `capteurs.json`** : seuls les capteurs explicitement
   marqués `"ingestion": true` sont décodés et publiés sur MQTT. Les autres
   sont détectés et signalés en log (ligne `🔕`) mais aucun message MQTT n'est émis.

Ce dernier niveau permet de distinguer :
- **Capteurs projet** (`ingestion: true`) : mesures publiées, appartiennent au parc surveillé.
- **Capteurs en attente de validation** (`ingestion: false`, valeur par défaut à
  l'auto-enregistrement) : détectés, enregistrés dans `capteurs.json`, mais exclus
  du flux MQTT tant que l'opérateur n'a pas validé.
- **Capteurs hors-projet** (voisin, autre tenant…) : même traitement que "en attente"
  — aucun spam MQTT.

Pour activer un capteur : éditer `capteurs.json`, passer `"ingestion": true`,
sauvegarder — le hot-reload applique le changement sur le prochain paquet BLE
reçu, **sans redémarrage**.

## 3. Identification et filtrage d'ingestion (`capteurs.json`)

Fichier externe avec hot-reload, qui associe chaque MAC à son identité, son
emplacement et ses flags de configuration. Structure complète d'une entrée :

```json
{
  "D2:0D:27:1C:F3:97": {
    "mac": "D2:0D:27:1C:F3:97",
    "nom": "disc-maxi-A03",
    "emplacement": "Atelier Troyes",
    "nom_mur": "",
    "nom_couche": "",
    "position": "",
    "ingestion": true,
    "lint_configure": true,
    "lint_max_confirme_s": 86400.0
  }
}
```

| Champ | Éditable | Description |
|---|---|---|
| `mac` | ❌ lecture seule | Adresse BLE (clé de sécurité — script vérifie cohérence clé/champ) |
| `nom` | ✅ | Nom lisible du capteur |
| `emplacement` | ✅ | Localisation physique textuelle |
| `nom_mur` | ✅ | Mur/paroi dans lequel le capteur est embarqué (ex. `Mur 1`, librement renommable) ; vide si non applicable (28/07/2026) |
| `nom_couche` | ✅ | Couche de la paroi (ex. `Milieu isolant`) ; vide si non applicable (28/07/2026) |
| `position` | ✅ | Position latérale sur le mur pour cette couche (ex. `Bas gauche`) ; vide si non applicable (28/07/2026) |
| `prestation` | ✅ | Référence de la prestation ou du contrat (ex. `C10517`) |
| `categorie R&D` | ✅ | Catégorie R&D associée (ex. `Hygrothermal`, `Retrait`, `Thermique`) |
| `ingestion` | ✅ | `true` = mesures publiées sur MQTT ; `false` (défaut) = exclu |
| `lint_configure` | ❌ auto | Positionné par `configure_capteurs.py` après succès GATT |
| `lint_max_confirme_s` | ❌ auto | Valeur d'intervalle de log confirmée (secondes) |

**`nom_mur`/`nom_couche`/`position` (28/07/2026)** : ajoutés pour permettre d'étiqueter
chaque capteur BLE embarqué dans une paroi (les capteurs HR/T du fichier
`data_HR_T/Données HR-T.xlsx` utilisé pour le POC de l'abaque 3D sont, dans le
produit final, ces mêmes capteurs BLE — voir section 18 pour la nuance : le
fichier Excel simule une base de données pour le POC uniquement, aucun lien
direct n'existe entre lui et `capteurs.json`). Contrairement au reste du
fichier, ces trois champs restent **libres et non contraints** (pas de liste
fermée de murs/couches/positions) — le nombre de murs/couches/positions du
système n'est donc jamais figé en dur, il se déduit de ce qui existe
réellement dans le registre. `latitude`/`longitude`/`altitude_m` ont été
retirés du schéma le même jour (jugés hors sujet pour ce projet).

**Logique de résolution du `capteur_id` affiché :**
1. `capteurs.json["<MAC>"]["nom"]` si renseigné
2. Sinon le `local_name` BLE annoncé par le capteur
3. Sinon `"Inconnu_<MAC>"`

**Auto-enregistrement :** tout nouveau capteur Blue Maestro détecté est
automatiquement ajouté avec `"ingestion": false` — il n'est pas publié sur
MQTT tant que l'opérateur ne passe pas ce flag à `true`.

→ Le hot-reload applique toute modification de `capteurs.json` sur le prochain
paquet BLE reçu, sans redémarrage du script.

## 4. Caractéristiques physiques des capteurs Blue Maestro Disc Maxi

Spécifications du modèle déployé dans MurMetric (`disc-maxi-alerts-003`, version 42),
confirmées par le manuel officiel Blue Maestro :

| Caractéristique | Valeur |
|---|---|
| Capteur interne | Silicon Labs SI7020 (T° + HR%) |
| Puce BLE | Nordic Semiconductor nRF52832 |
| Plage de température | -40°C à +120°C |
| Précision température | ±0.3°C maximum |
| **Type de pile** | **CR2477** (pile bouton, non CR2032) |
| **Autonomie pile** | **4 à 5 ans** selon usage et réglages |
| Portée BLE | 75 m en ligne de mire |
| Stockage interne | 8+ mois avec réglages par défaut |
| **Intervalle de log configurable** | **60 s à 86 400 s (24 h)** — défaut usine : 3 600 s (1 h) |
| Certifications | CE, FCC |

⚠️ La pile est une **CR2477** (plus grande que CR2032, plus haute capacité) — ne pas
confondre lors du remplacement.

## 5. Décodage du protocole Blue Maestro

Les octets `0x33, 0x01` (company ID) sont déjà retirés par `bleak` :
`payload_bytes[0]` correspond directement à l'octet **version** du protocole.

| Version | Mesures disponibles | Encodage | Résolution |
|---|---|---|---|
| 13 | Température | big-endian | 0.1 |
| 23 | Température + Humidité | big-endian | 0.1 |
| 27 | Température + Humidité + Pression | big-endian | 0.1 |
| 41 | Température (haute précision) | little-endian | 0.01 |
| **42** (`disc-maxi-A03`) | **Température + Humidité** (haute précision) | little-endian | 0.01 |
| 43 | Température + Humidité + Pression (haute précision) | little-endian | 0.01 |

Offsets dans `payload_bytes` (entête retirée) :
- **v41/42/43** : température octets 15-16, humidité octets 17-18
- **v13/23/27** : température octets 6-7, humidité octets 8-9

Le **point de rosée** n'est pas transmis par le Disc Maxi v42 — calculé côté
script (formule de Magnus-Tetens, `a=17.27, b=237.7`). La version 23 le transmet
directement dans le paquet.

La **batterie (%)** est lue à `payload_bytes[1]` (uint8).

L'**intervalle de log** est exposé dans `intervalle_log_secondes` :
- v41/42/43 : `payload_bytes[2:6]`, uint32 LE, en déciseconde (÷10 → secondes). Validé (900 s sur Disc Maxi A03).
- v13/23/27 : `payload_bytes[2:4]`, uint16 BE, en secondes. Non vérifié empiriquement.

## 6. Mesure de l'intervalle entre paquets

Le script mémorise (`dernieres_detections`, dict MAC → timestamp) l'instant
de la dernière détection par capteur pour afficher l'intervalle entre paquets.

Observations empiriques :
- Intervalle réel : 2 à 60 s (irrégulier — duty cycle scan Windows).
- Deux callbacks quasi simultanés (~0.00 s) → advertisement + scan response.
- Artefact Windows/bleak : redétection toutes les ~60 s avec RSSI -127 dBm
  (sentinelle) et octets identiques. Rejetée via `RSSI_MIN_VALIDE = -100`.

## 7. Structure du message publié sur MQTT

Topic : `frd/capteurs/bruts` (configurable via `MQTT_TOPIC`)

```json
{
  "capteur_id": "disc-maxi-A03",
  "emplacement": "Atelier Troyes",
  "mac": "D2:0D:27:1C:F3:97",
  "horodatage": "01/07/2026 11:15:54",
  "temperature_c": 23.83,
  "humidite_percent": 72.62,
  "point_de_rosee_c": 18.53,
  "batterie_percent": 100,
  "rssi_dbm": -37,
  "intervalle_log_secondes": 86400.0,
  "liste_chiffres": [42, 100, 0, 47, 13, 0, ...]
}
```

- `horodatage` : heure de réception côté script (pas celle du capteur).
- `liste_chiffres` : octets bruts pour debug/audit — non stockés en base.

**Artefact RSSI filtré** : RSSI ≤ -100 dBm → détection fantôme du cache Windows
rejetée avant tout traitement.

## 8. Protocole de configuration active (GATT BLE)

Implémenté dans `configure_capteurs.py`. `ingestion_capteurs_bluetooth.py`
reste en écoute passive uniquement.

- **Service** : UUID `6E400001-B5A3-F393-E0A9-E50E24DCCA9E`
- **Écriture des commandes (host→device)** : `6E400002` → `[write, write-without-response]`
- **Réponses du capteur (device→host)** : `6E400003` → `[notify]`

⚠️ Ces UUIDs sont **inversés** par rapport au standard Nordic UART.

| Génération | Format commande | Réponse | Min | Max |
|---|---|---|---|---|
| **v41/v42/v43** (Disc Maxi) | `setlog~<secondes>` | `OK: Logging interval updated successfully` | 1 s | **86400 s (24 h)** |
| v13/v23/v27 (legacy) | `*lint<secondes>` | `Command Recognised` | 2 s | **86400 s (24 h)** |

**Sensor/advertising interval :**
- v13/23/27 : `*sint<secondes>` — contrôle la fréquence de mesure ET d'advertising
- v41/42/43 : **non exposé** dans l'API publique (~10 s par défaut)

**Référence complète des commandes legacy (v13/v23/v27) — source : bluemaestro.com/developer :**

| Commande | Format | Description |
|---|---|---|
| `*lint` | `*lint<secondes>` | Intervalle de log (2–86400 s) |
| `*sint` | `*sint<secondes>` | Intervalle capteur/advertising |
| `*logall` | `*logall` | Télécharger tous les logs |
| `*logtemp` | `*logtemp<pos>` | Log température depuis position |
| `*loghumi` | `*loghumi<pos>` | Log humidité depuis position |
| `*logdewp` | `*logdewp<pos>` | Log point de rosée depuis position |
| `*clr` | `*clr` | Effacer tous les logs |
| `*pwd` | `*pwd<4 chiffres>` | Définir/entrer mot de passe |
| `*nam` | `*nam<nom>` | Renommer le device (max 8 chars) |
| `*txp` | `*txp<0-2>` | Puissance TX (0=+4 dBm, 1=0 dBm, 2=-4 dBm) |
| `*led` | `*led` | Toggle LED |
| `*airon` / `*airoff` | — | Mode avion (on/off) |
| `*shp` | `*shp` | Veille profonde (shipping/deep sleep) |
| `*rboot` | `*rboot` | Redémarrer le device |
| `*dfu` | `*dfu` | Mode bootloader ⚠️ |
| `*qq` | `*qq` | Forcer la déconnexion |
| `*info` | `*info` | Lire les paramètres du device |
| `*tell` | `*tell` | Données télémétriques en streaming |
| `*batt` | `*batt` | Niveau de batterie |
| `*bur` | `*bur` | Mode burst/streaming |
| `*unitsc` / `*unitsf` | — | Unités °C ou °F |
| `*alrm1t` / `*alrm2t` | `*alrm1t<op><val>` | Seuil alarme température 1/2 |
| `*alrm1h` / `*alrm2h` | `*alrm1h<op><val>` | Seuil alarme humidité 1/2 |
| `*alrmi` | `*alrmi` | Infos alarmes |
| `*alrmclr` | `*alrmclr` | Effacer les alarmes |

## 9. Script de configuration active (`configure_capteurs.py`)

Configure l'intervalle de log de chaque capteur à sa valeur maximale pour
maximiser la durée de vie des piles. Lancé automatiquement par `start.py`
au démarrage, et périodiquement par `ingestion_capteurs_bluetooth.py`.

**Séquence d'exécution :**
1. **Scan passif 30 s** → inventorie les capteurs via `BLEDevice` complets.
2. **Capteurs déjà configurés** (`lint_configure: true`) → ignorés sans connexion.
3. **Connexion GATT active** (BleakClient) → envoi de la commande sur `6E400002` (write) :
   - v41/42/43 : `setlog~86400` → max **86400 s (24 h)**
   - v13/23/27 : `*lint86400\r\n` → max **86400 s (24 h)**
4. **Déconnexion du capteur** = signal que la commande a été acceptée.
5. **Vérification** → scan passif ciblé, lit `intervalle_log_secondes` dans
   le prochain paquet advertising. Valeur réellement appliquée stockée.
6. **Mise à jour de `capteurs.json`** → `lint_configure: true` et `lint_max_confirme_s`.

**Champs ajoutés dans `capteurs.json` après configuration :**
```json
{
  "D2:0D:27:1C:F3:97": {
    "mac": "D2:0D:27:1C:F3:97",
    "nom": "disc-maxi-A03",
    "emplacement": "Atelier Troyes",
    "ingestion": true,
    "lint_configure": true,
    "lint_max_confirme_s": 86400.0
  }
}
```

## 10. Points d'entrée

### `start.py` — Raspberry Pi

Lance la séquence BLE au démarrage :
1. `configure_capteurs.py` (configuration GATT des capteurs non encore traités)
2. `ingestion_capteurs_bluetooth.py` (ingestion continue)

Le bridge `bridge_mqtt_to_kafka.py` **n'est pas lancé ici** — il tourne sur le VPS
dans son propre conteneur Docker pour éviter les doublons d'écriture dans InfluxDB.

**Usage :** `python start.py`

### `start_dewesoft.py` — PC labo Windows

Vérifie la présence de `DSRemoteConnect64.dll` puis lance `ingestion_dewesoft.py`.
Publie sur le même broker MQTT cloud que le RPi, avec son propre buffer SQLite.

**Usage :** `python start_dewesoft.py`

## 11. Reconfiguration périodique (intégrée dans `ingestion_capteurs_bluetooth.py`)

Une tâche asyncio (`tache_reconfiguration_periodique`) tourne en arrière-plan
pendant l'ingestion. Toutes les **6 heures** (configurable via `RECONF_INTERVAL`) :
- Si tous les capteurs ont `lint_configure: true` → log discret, rien à faire.
- Si des capteurs manquent ce flag → **pause du scanner d'ingestion** →
  `configure_capteurs.py` en sous-processus (30 s scan + GATT) → **reprise**.

**Configurer l'intervalle :** `set RECONF_INTERVAL=3600 && python start.py` (1 h).

## 12. Pipeline cloud : MQTT → Kafka → InfluxDB

### `bridge_mqtt_to_kafka.py`

Souscrit aux trois topics MQTT et transfère les messages bruts dans Kafka
sans transformation. Un topic Kafka par type de source, namespaced par tenant :

| Topic MQTT | Topic Kafka |
|---|---|
| `frd/capteurs/bruts` | `murmetric.{tenant}.capteurs.bruts` |
| `frd/capteurs/registre` | `murmetric.{tenant}.capteurs.registre` |
| `frd/dewesoft/bruts` | `murmetric.{tenant}.dewesoft.bruts` |

### `kafka_consumer_influx.py`

Consomme les trois topics Kafka et écrit dans InfluxDB en **mode batch asynchrone**
(`batch_size=500`, `flush_interval=1 s`). Adapté au débit DeweSoftX (1 msg/s/canal).

Reprise sur panne : si InfluxDB redémarre, le consumer group reprend depuis le
dernier offset Kafka committé — aucune perte de données (rétention 7 jours).

**Modèle de données** (mesure : `mesures_capteurs`) :

| Champ | Type InfluxDB | Source MQTT | Description |
|---|---|---|---|
| `adresse_mac` | Tag | `mac` | Adresse BLE (filtrage/groupement) |
| `nom_capteur` | Tag | `capteur_id` | Nom lisible (filtrage/groupement) |
| `emplacement` | Tag | `emplacement` | Zone physique (filtrage/groupement) |
| `temperature` | Field float | `temperature_c` | °C |
| `humidite` | Field float | `humidite_percent` | % |
| `point_de_rosee` | Field float | `point_de_rosee_c` | °C (si calculable) |
| `batterie` | Field int | `batterie_percent` | % |
| `rssi` | Field int | `rssi_dbm` | dBm |

Champs MQTT non stockés : `horodatage` (InfluxDB gère son propre timestamp),
`intervalle_log_secondes` (audit uniquement), `liste_chiffres` (debug).

**Mesure `registre_capteurs`** (topic : `frd/capteurs/registre`) :

| Champ | Type InfluxDB | Description |
|---|---|---|
| `adresse_mac` | Tag | Identifiant unique du capteur |
| `nom` | Tag | Nom lisible (depuis capteurs.json) |
| `emplacement` | Tag | Zone physique (depuis capteurs.json) |
| `nom_mur` | Tag | Mur/paroi (depuis capteurs.json, vide si non applicable) |
| `nom_couche` | Tag | Couche de la paroi (depuis capteurs.json, vide si non applicable) |
| `position` | Tag | Position sur le mur (depuis capteurs.json, vide si non applicable) |
| `prestation` | Tag | Référence de la prestation ou du contrat |
| `categorie R&D` | Tag | Catégorie R&D associée |
| `ingestion` | Field bool | true = mesures publiées sur MQTT |
| `lint_configure` | Field bool | true = intervalle de log optimisé |
| `lint_max_confirme_s` | Field float | Intervalle de log confirmé (secondes) |

Publiée sur le topic `frd/capteurs/registre` :
- Au démarrage (connexion MQTT initiale)
- À chaque modification de `capteurs.json` (hot-reload)

Tous les capteurs sont publiés dans ce registre, y compris ceux avec
`ingestion: false` — permettant à l'application cliente d'avoir une vue
complète du parc, même des capteurs non encore validés.

Requête InfluxDB pour la config actuelle d'un capteur :
```flux
from(bucket: "Test_Capteurs")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "registre_capteurs")
  |> last()
  |> pivot(rowKey: ["adresse_mac"], columnKey: ["_field"], valueColumn: "_value")
```

**Mesure `mesures_dewesoft`** (topic Kafka : `murmetric.{tenant}.dewesoft.bruts`) :

| Champ | Type InfluxDB | Description |
|---|---|---|
| `source` | Tag | Toujours `"dewesoft"` |
| `canal_nom` | Tag | Nom du canal DeweSoftX (ex. `VA1`) |
| `canal_unite` | Tag | Unité de mesure (ex. `mm/m`) |
| `valeur` | Field float | Valeur brute de la mesure de retrait (jamais modifiée) |
| `valeur_filtree` | Field float | Valeur après filtre de Hampel anti-vibration (cf. section 17) |
| `est_aberrant` | Field bool | true si ce point a été corrigé par le filtre |
| `canal_index` | Field int | Index du canal dans DeweSoftX |
| `taux_echantillonnage` | Field float | Fréquence d'acquisition (Hz) |

## 13. Résilience — SQLite local + Kafka

La résilience est assurée sur **deux segments distincts** par deux mécanismes
complémentaires :

| Segment | Mécanisme | Scénario couvert |
|---|---|---|
| **Terrain → VPS** | SQLite local (RPi / PC Windows) | Réseau internet coupé, VPS inaccessible |
| **Mosquitto → InfluxDB** | Kafka (rétention 7 jours) | InfluxDB crash, redémarrage du consumer |

Chaque machine terrain (RPi et PC Windows) possède son propre `murmetric_buffer.db`,
indépendant. Aucun partage de fichier entre les deux machines n'est nécessaire.

### Buffer SQLite local

En cas d'indisponibilité du broker MQTT cloud (VPS down, perte internet),
les mesures sont accumulées localement et renvoyées à la reconnexion.

**Cas de panne gérés :**

| Panne | Détection | Comportement |
|---|---|---|
| Perte internet | `publish()` échoue | → SQLite |
| Service MQTT VPS down | Connexion TCP perdue | → SQLite |
| VPS entier down | Idem | → SQLite |

**Fichier buffer :** `murmetric_buffer.db` (SQLite, même répertoire que les scripts)

**Schéma :**
```sql
CREATE TABLE buffer_mqtt (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT    NOT NULL,
    payload     TEXT    NOT NULL,
    horodatage  TEXT    NOT NULL,
    tente_le    TEXT    DEFAULT NULL
)
```

**Cycle de synchronisation (tâche `tache_sync_sqlite`) :**
1. Déclenché immédiatement à la reconnexion MQTT (`on_connect` callback).
2. Déclenché périodiquement toutes les `SYNC_INTERVAL` secondes (défaut 30 s).
3. Purge des messages expirés (> `SQLITE_RETENTION` jours, défaut 7 j).
4. Lecture par batch de `SYNC_BATCH_SIZE` messages (défaut 50) ORDER BY horodatage ASC.
5. Publication QoS 1 + `wait_for_publish(timeout=5s)`.
6. Suppression de SQLite **après** confirmation de publication.
7. Si messages restants → re-déclenchement immédiat.

**Variables d'environnement :**

| Variable | Défaut | Description |
|---|---|---|
| `MQTT_BROKER` | `localhost` | Adresse du broker MQTT cloud (VPS) |
| `SQLITE_RETENTION` | `7` | Rétention du buffer en jours |
| `SYNC_BATCH_SIZE` | `50` | Messages par batch de rattrapage |
| `SYNC_INTERVAL` | `30` | Secondes entre deux tentatives de sync |

**Affichage dans les logs :**
- `☁️ MQTT cloud` → mesure publiée directement sur le VPS
- `💾 SQLite local` → mesure stockée localement en attente

## 14. Configuration environnement PC labo Windows — pilotage COM/DCOM de DeweSoftX

**⚠️ Historique — voie abandonnée le 04/08/2026.** La méthode live/COM
(`ingestion_dewesoft.py`, `DSRemoteConnect64.dll`/`.dll`, `test_dewesoft_com.py`,
dépendance `pywin32`) a été **définitivement retirée du dépôt**, plus
seulement mise en réserve : la méthode d'import de fichiers `.dxd`
(`ingestion_dewesoft_dxd.py`) est désormais la SEULE méthode d'ingestion
DeweSoftX. Section conservée pour l'historique de l'investigation
COM/DCOM (utile si le besoin de temps réel revient un jour), mais ne
décrit plus une voie active du projet.

Configuration réalisée le 15/07/2026 sur le PC d'acquisition du laboratoire
d'Amiens, en vue du pilotage automatique de DeweSoftX (version 2023.6) et de
l'extraction de données via Python — **complémentaire** au fichier
`DSRemoteConnect64.dll` déjà présent à la racine du repo.

### Windows

- **.NET Framework** activés via *Fonctionnalités Windows* : 3.5 et
  4.8 Advanced Services.
- **Serveur COM/DCOM de DewesoftX enregistré** (invite CMD Administrateur) :
  ```
  cd "C:\Program Files\DewesoftX\Bin64"
  DEWESoft.exe /regserver
  ```
  Nécessaire car l'exécutable principal se trouve dans un sous-dossier
  spécifique — l'enregistrement automatique standard ne suffit pas.

### Python

- **Python 3.11** installé avec l'option *Add python.exe to PATH*.
- **Bibliothèque de liaison retenue : `pywin32`** (`pip install pywin32`),
  via l'architecture standard COM/OLE de Windows.

**Décision technique — pourquoi pas `pythonnet`/`clr` :** l'approche par le
moteur .NET géré a été écartée car `DSNET64.dll` (`\Addons64\DSNET`) est une
bibliothèque **native C++ sans manifeste d'assembly**, donc non chargeable
proprement par `pythonnet`. L'API COM/OLE exposée par DewesoftX (ProgID
`Dewesoft.App`) est en revanche parfaitement supportée par `pywin32`.

### Script de validation (test d'intégration réussi)

```python
import win32com.client
dewesoft = win32com.client.Dispatch("Dewesoft.App")
version = dewesoft.Version  # → confirmé : 2023.6
```

Exécuté avec succès sur le PC d'Amiens — connexion COM au noyau DeweSoftX
confirmée.

### Statut

L'environnement Python d'Amiens dispose désormais des privilèges nécessaires
pour, via COM (`Dewesoft.App`) : charger des configurations `.dxs`,
démarrer/arrêter des enregistrements, et orchestrer l'ingestion des données.
Cette "tuyauterie" est validée à 100 % côté connexion ; **reste à décider**
son intégration dans le pipeline d'ingestion existant (cf. points ouverts).

## 15. Granularité temporelle de l'abaque 3D (conception — non implémenté)

Le prototype de l'abaque 3D (T, HR, retrait, teneur en eau, point de rosée —
cf. artefact partagé séparément) propose un sélecteur d'unité de temps
Heure/Jour. Dans le prototype, c'est un simple changement d'affichage (division
par 24) car les données y sont un petit jeu réel mais pré-calculé (JSON
statique, cf. section 18 — pas de lecture InfluxDB en direct). **En
production, avec des données InfluxDB horodatées à la seconde sur plusieurs
mois, ce ne peut pas être un relabeling côté client — il faut une
ré-agrégation côté serveur.**

### Mécanisme : `aggregateWindow()` (Flux)

```flux
from(bucket: "Test_Capteurs")
  |> range(start: v.startTime, stop: v.stopTime)
  |> filter(fn: (r) => r._measurement == "mesures_capteurs" or r._measurement == "mesures_dewesoft")
  |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
```

Le paramètre `every` correspond directement au sélecteur de granularité de
l'interface. Flux supporte nativement 4 pas pertinents ici :

| Granularité UI | `every` Flux | Remarque |
|---|---|---|
| Heure          | `1h`         | — |
| Jour           | `1d`         | — |
| Semaine        | `1w`         | — |
| Mois           | `1mo`        | Calendaire (respecte la vraie longueur du mois, pas fixé à 30 j) |

### Contraintes à respecter à l'implémentation

- **Une seule fenêtre `every` pour toutes les variables affichées** (T, HR,
  retrait, point de rosée) — c'est ce qui garantit qu'elles restent alignées
  sur les mêmes bornes temporelles, même si les capteurs BLE et DeweSoftX ont
  des cadences natives différentes à la source.
- **Point de rosée** : déjà un champ stocké (`point_de_rosee` dans
  `construire_point_capteurs()`), s'agrège directement comme T/HR.
- **Teneur en eau** : n'est *pas* un flux continu comme T/HR/retrait — c'est
  une saisie manuelle éparse (cf. section 16). `aggregateWindow(fn: mean)`
  n'est donc pas pertinent pour elle : la plupart des fenêtres n'auraient
  aucun point à moyenner. Voir section 16 pour la jointure "au plus proche"
  à utiliser à la place quand elle est croisée avec T/HR dans l'abaque.
- **Changer de granularité doit redéclencher une requête**, pas juste
  reformater l'axe — le paramètre proposé côté API : `granularite=heure|jour|semaine|mois`
  sur l'endpoint qui sert les données à l'abaque, traduit en `every` Flux
  côté backend.
- **Coupler la granularité à la fenêtre temporelle affichée** : "Heure" n'a de
  sens que sur une fenêtre resserrée (quelques semaines) ; sur 6 mois de
  données, forcer "Heure" produirait des milliers de points par variable pour
  un gain de lisibilité nul. À terme, la granularité fine pourrait être
  désactivée/déconseillée au-delà d'une certaine largeur de fenêtre plutôt que
  laissée comme un réglage totalement indépendant.

## 16. Teneur en eau — saisie manuelle (conception — non implémenté)

Contrairement à T/HR (capteurs BLE) et au retrait (DeweSoftX), la teneur en
eau n'est mesurée par aucun capteur en continu dans ce projet — elle est
relevée ponctuellement sur le terrain (type humidimètre à pointes/Protimeter)
et **saisie manuellement par un utilisateur depuis l'appli**.

### Mesure InfluxDB : `mesures_teneur_eau`

| Champ | Type InfluxDB | Description |
|---|---|---|
| `utilisateur_id` | Tag | Identifiant de l'utilisateur ayant saisi la mesure |
| `utilisateur_nom` | Tag | Nom affiché au moment de la saisie (dénormalisé — volontairement figé même si l'utilisateur renomme son compte plus tard : c'est un historique d'audit, pas un profil vivant) |
| `mur` | Tag | Mur / point de mesure concerné |
| `couche` | Tag | Couche de la paroi où la mesure est prise (ex. `Carreau/ext`, `Milieu carreau`, `Milieu isolant`… — même notion que `nom_couche` dans `capteurs.json`/`capteurs_retrait.json` et l'axe couche de l'abaque, valeurs libres non contraintes). Ajouté le 29/07/2026 pour distinguer par ex. carreau intérieur/extérieur. Volontairement pas nommé `position` : ce mot désigne déjà la position **latérale** sur le mur ailleurs dans le projet — le réutiliser ici pour la profondeur aurait créé une ambiguïté |
| `prestation` | Tag | Référence prestation (cohérence avec `registre_capteurs`) |
| `teneur_eau_pourcent` | Field (float) | Valeur mesurée (%) |
| `commentaire` | Field (string, optionnel) | Note libre (ex. "mesuré après pluie") |
| `date_mesure` | — (devient `_time`) | Date/heure de la mesure **terrain**, saisie par l'utilisateur (par défaut "maintenant" dans le formulaire, modifiable) |

**`_time` = `date_mesure`, pas l'horodatage d'insertion.** Revu suite à
discussion : plus d'horodatage automatique à l'écriture — le point est écrit
avec `.time(date_mesure)` explicitement (même mécanisme que
`construire_point_dewesoft()` pour l'import `.dxd`, section 12), afin que la
donnée porte la date réelle de la mesure sur le terrain et non celle, sans
intérêt scientifique, du moment où l'utilisateur a ouvert l'appli pour la
saisir.

⚠️ Conséquence à connaître : sans horodatage d'insertion séparé, il n'y a plus
de trace de "quand la saisie a eu lieu dans l'appli" — seulement de "quand la
mesure a été prise". Si un jour un audit de saisie (traçabilité des
manipulations dans l'outil, indépendamment de la date terrain) devient utile,
il faudra un champ supplémentaire dédié à ce moment-là ; ce n'est pas prévu
pour l'instant.

### Chemin d'écriture

Formulaire de saisie (appli) → API backend → écriture directe InfluxDB.
**Pas de passage par Kafka** : contrairement aux flux capteurs continus, une
saisie humaine est un événement isolé et à faible volume, la résilience
Kafka/SQLite n'apporte rien ici — une simple écriture InfluxDB directe
suffit.

⚠️ **Sécurité** : `utilisateur_id`/`utilisateur_nom` doivent être résolus
côté serveur depuis la session authentifiée, **jamais** acceptés tels quels
depuis un champ du formulaire — sinon un utilisateur pourrait soumettre une
mesure au nom d'un autre.

### Correction d'une saisie existante (conception, 12/08/2026)

Question posée : l'utilisateur doit pouvoir corriger une saisie déjà en
base (erreur de frappe sur la valeur, mauvaise paroi/couche sélectionnée,
mauvaise date). InfluxDB ne s'y prête pas de la même façon selon ce qui
change :

- **Corriger un field** (`teneur_eau_pourcent`, `commentaire`) : trivial —
  réécrire un point avec exactement la même mesure/tags/timestamp mais une
  valeur de field différente **remplace** l'ancienne (dernière écriture
  gagne). Une simple écriture suffit, pas de suppression nécessaire.
- **Corriger un tag** (`mur`, `couche`) **ou la date** (`date_mesure`,
  devient `_time`) : un tag et le timestamp font partie de l'**identité**
  du point — les changer ne "renomme" rien, ça **crée un point distinct**,
  l'ancien reste orphelin à côté. Nécessite un **delete + réécriture**
  (suppression par prédicat exact sur les anciens tags/timestamp, puis
  écriture du point corrigé) — même mécanisme que la réconciliation MAC du
  backfill HR/T (section 29), pas une UPDATE au sens SQL.

**Conséquence pour le formulaire d'édition** : contrairement à une base
relationnelle avec un ID auto-incrémenté, il n'existe pas d'identifiant
unique de ligne dans InfluxDB. Le formulaire d'édition doit connaître le
**triplet exact (mur, couche, date_mesure)** du point visé pour pouvoir le
cibler précisément lors d'une correction de tag/date — ce triplet doit donc
rester unique (pas deux saisies pour le même mur+couche+date_mesure) et
être conservé côté frontend entre l'affichage et la soumission de la
correction. Non implémenté à ce stade — conception uniquement, la saisie
initiale (section précédente) reste la seule brique déjà spécifiée pour la
production.

### Conséquence sur l'abaque 3D

La teneur en eau est un flux **épars** (quelques points par mur), alors que
T/HR/retrait sont denses. Les croiser dans l'espace de phase demande une
**jointure "au plus proche"** (Flux `join()` sur le point T/HR le plus proche
en temps de chaque saisie de teneur en eau), pas un `aggregateWindow` par
moyenne comme section 15 — la plupart des fenêtres n'auraient sinon aucune
valeur à moyenner.

### Intégration POC de données réelles (31/07/2026)

Le paragraphe ci-dessus décrit la **conception de production** (saisie
manuelle par formulaire, écriture InfluxDB `mesures_teneur_eau`) — toujours
non implémentée. Séparément, l'utilisateur a fourni un relevé réel de
teneur en eau (`data_teneur/Teneur en eau Paroi.xlsx`, prélèvements au
Protimeter/humidimètre à pointes), qui a été intégré dans l'abaque 3D **à la
place de** l'ancienne valeur synthétique `6 + 0.12 × humidité` (formule
placeholder sans rapport avec une vraie mesure, seulement là pour que l'axe
ait une courbe à tracer).

**Structure du fichier source** (feuille "Feuil1", 11 prélèvements du
21/11/2025 au 18/03/2026, par mur et par couche) :
- Mur **"normal"** = SOCMA 1 → **Mur 1** ; mur **"gros gravier"** = SOCMA2 →
  **Mur 2** (mapping confirmé par l'utilisateur le 31/07/2026 — la
  numérotation "SOCMA 1/SOCMA2" de ce fichier est différente de la
  convention "SOCMA 2 = Mur 1 / SOCMA 2BIS = Mur 2" du fichier HR/T,
  section 18, donc pas déductible automatiquement).
- 3 couches par mur ("Carreau ext" / "Carreau int" / "Isolant"), un
  sous-ensemble des 5 couches T/HR (carreau_ext, milieu_carreau,
  carreau_isolant, milieu_isolant, isolant_osb) — mappées ainsi (meilleure
  hypothèse physique, confirmée par l'utilisateur) :
  `Carreau ext → carreau_ext`, `Carreau int → carreau_isolant`,
  `Isolant → milieu_isolant`. **`milieu_carreau` et `isolant_osb` n'ont
  aucune donnée réelle de teneur en eau** — l'abaque affiche "—" pour ces
  deux couches plutôt qu'une valeur inventée.

**Pipeline** : nouveau script `data_reel_compile/extraire_teneur_eau_reel.py`
(parse le classeur, détecte la ligne d'en-tête des dates dynamiquement plutôt
que des numéros de ligne en dur, produit `teneur_eau_reel.json`, 64 mesures).
`combiner_donnees_reelles.py`/`combiner_donnees_reelles_3h.py` font ensuite
la jointure "au plus proche dans le temps" par (mur, couche) — **sans limite
de distance** : un jour sans nouveau prélèvement garde la dernière valeur
connue la plus proche, même si elle date de plusieurs semaines (ex. le jeu
de données daily va jusqu'au 26/05/2026, ~2 mois après le dernier
prélèvement du 18/03/2026 — la valeur affichée ce jour-là est donc une
estimation tenue, pas une mesure fraîche ; à garder en tête en lisant le
graphique). Chaque entrée finale porte `teneurEau` (valeur de la couche
"milieu isolant", cohérente avec la valeur par défaut de T/HR) et
`teneurEauCouches.{carreau_ext,carreau_isolant,milieu_isolant}`.

**Dans l'abaque** : contrairement à T/HR, la teneur en eau réelle **ne varie
pas par position** (bas gauche/milieu, haut milieu/droite) — seulement par
couche — puisqu'elle n'a été mesurée qu'une fois par mur et par couche, pas
par position sur le mur. `construireDataReelle()`, `appliquerPositionTH()`,
`axisValue()` et l'infobulle de survol ont tous été mis à jour pour lire
`teneurEauCouches[couche]` (position ignorée pour cet axe) au lieu de
l'ancienne formule. Conséquence UX : comparer les **positions** avec la
teneur en eau sur un axe produit plusieurs courbes identiques (aucune valeur
ne varie), documenté dans l'astuce du panneau "Comparer plusieurs positions"
plutôt que masqué ; comparer les **couches**, en revanche, est désormais
pleinement pertinent (3 courbes réellement différentes).

## 17. Filtrage anti-vibration (Hampel) — implémenté

Les capteurs de retrait sont sensibles aux vibrations (choc/passage à
proximité), ce qui crée des pics ponctuels dans les courbes — confirmé sur
les fichiers réels de `data_retrait/` : un pic isolé jusqu'à ~17000x le bruit
normal lors d'un essai de calibration (21/11/2025), simultané sur les 8
canaux (signature d'une perturbation externe, pas d'une panne isolée).

### Algorithme (`filtrer_hampel()` dans `ingestion_dewesoft_dxd.py`)

Filtre de Hampel : médiane + MAD (écart absolu médian) glissants sur une
fenêtre courte (±`HAMPEL_FENETRE` échantillons, défaut 10). Un point est
remplacé par la médiane de sa fenêtre si son écart dépasse
`HAMPEL_SEUIL_K × MAD` (défaut 8×).

**Deux itérations ratées avant la version retenue**, pour mémoire :
1. Un plancher à 0 (pas de plancher) : loupait les pics quand la fenêtre
   contenait beaucoup de valeurs identiques (quantification du capteur) —
   le MAD glissant tombait exactement à zéro et désactivait la détection au
   lieu de la déclencher.
2. Un plancher basé sur le MAD global du fichier entier : corrigeait le
   problème 1 mais donnait des faux positifs (confondait le bruit de
   quantification normal avec un vrai pic) sur certains canaux, et un
   plancher à 10 % du MAD global loupait les pics modérés en fonctionnement
   courant. Une fenêtre secondaire plus large (~600 échantillons) réglait la
   précision mais rendait le traitement ~30x plus lent (impraticable sur un
   fichier de 432 000 échantillons/canal).

**Version retenue** : le plancher utilise le MAD des *différences
successives* dans la même petite fenêtre (pas une fenêtre séparée) — coût
négligeable, insensible à la dérive lente du signal, et ne s'effondre pas à
zéro aussi facilement qu'un MAD sur les valeurs brutes. Validé sur les
fichiers réels : zéro faux positif sur les enregistrements propres, détection
correcte du pic de calibration sur les 8 canaux. ~4s/canal sur un fichier de
12h (432 000 échantillons) — acceptable pour un traitement par lot toutes les
12h.

### Ajustabilité du seuil — ce qui est fait vs ce qui reste à faire

`HAMPEL_SEUIL_K`/`HAMPEL_FENETRE` sont des variables d'environnement,
réglables **au déploiement**, pas encore par l'utilisateur final **depuis
l'application**. Rendre le seuil ajustable en direct depuis l'interface
demande de recalculer le filtre à la demande à partir de `valeur` (toujours
brute) côté backend/API — composant qui n'existe pas encore dans ce dépôt.
En attendant, changer le seuil impose de retraiter les `.dxd` sources
(conservés dans `DXD_PROCESSED_FOLDER`, jamais supprimés).

### Interface (maquette)

La maquette de l'abaque 3D superpose les deux vues plutôt qu'un simple
interrupteur brut/filtré : nuage de points bruts pâle en arrière-plan,
trajectoire filtrée en trait plein par-dessus — montre visuellement *ce qui a
été retiré*. Un curseur "sensibilité" y simule l'ajustement du seuil
(recalcul en direct côté client) — préfigure l'ajustement en direct réel, qui
nécessitera le backend évoqué ci-dessus. Ce filtrage tourne sur le jeu de
données réelles décrit en section 18 (l'artefact n'a plus de jeu simulé
depuis le 24/07/2026 — cf. section 18).

## 18. Abaque 3D — intégration des données réelles (POC, implémenté)

L'artefact de l'abaque 3D trace les données effectivement mesurées, pour
valider que la visualisation tient sur de vraies données avant tout
développement backend. Un jeu de données **simulé** a coexisté un temps avec
le jeu réel (bouton Simulé/Réel, pour démontrer le déphasage HR→teneur en
eau→retrait avant que les vraies données ne soient prêtes) — **retiré le
24/07/2026** avec le panneau "Administration" (mock utilisateurs/permissions,
lui aussi supprimé) : consigne explicite que cette interface ne sert qu'au
monitoring/filtrage/visualisation par l'utilisateur final, pas à une
démonstration multi-mode ni à la gestion des accès.

**Important (précisé le 31/07/2026)** : tout le pipeline `data_reel_compile/`
(scripts d'extraction + fichiers JSON intermédiaires + fichier Excel
`data_HR_T/Données HR-T.xlsx`) est un outillage **exclusivement POC**, sans
équivalent dans le produit final. Dans la version finale :
- chaque fichier `.dxd` est extrait **automatiquement dès sa génération**
  par `ingestion_dewesoft_dxd.py` (section 12/19) — pas de compilation par
  lot ni de fenêtre de dates à gérer ;
- les mesures BLE HR/T sont stockées **automatiquement en base** (InfluxDB)
  par `ingestion_capteurs_bluetooth.py`, en continu — pas de classeur Excel.

Le classeur Excel et l'extraction de plusieurs `.dxd` à la fois n'existent
donc que pour simuler, une fois, un jeu de données réaliste à partir duquel
construire et démontrer l'abaque — à ne jamais confondre avec l'architecture
d'ingestion réelle (section 12).

### Mapping capteurs → position → mur

Le fichier `data_HR_T/Données HR-T.xlsx` (55 capteurs T+HR numérotés) a été
recoupé avec les onglets "Courbes de suivi T"/"Courbe de suivi HR" en
inspectant le XML des graphiques Excel (`<c:f>` des séries) pour retrouver
quel numéro de capteur correspond à quelle position physique du mur. Résultat :

- **SOCMA 2 = Mur 1**, **SOCMA 2BIS = Mur 2** (confirmé — SOCMA est juste une
  autre appellation du mur, le POC garde le nom "Mur 1/2").
- Chaque mur a 4 positions non ambiguës : *bas gauche, bas milieu, haut
  milieu, haut droite*. Une 5<sup>e</sup> position, *centre milieu*, existe
  mais est **partagée entre les deux murs** dans le classeur (même capteur
  référencé sur les deux graphiques) — exposée séparément plutôt que
  rattachée arbitrairement à un mur.
- Chaque position a plusieurs profondeurs (carreau/isolant, milieu isolant,
  isolant/OSB, milieu carreau…) ; la couche **« milieu isolant »** a été
  retenue comme représentative pour le POC.

### Pipeline d'extraction (`data_reel_compile/`)

Trois scripts, rejouables tels quels (lisent/écrivent dans ce dossier) :

| Script | Rôle |
|---|---|
| `extraire_retrait_reel.py` | Parcourt `data_retrait/*.dxd` (SDK DWDataReader), médiane robuste par fichier et par canal (8 canaux : HA1/VA1/HB1/VB1 mur 1, HA2/VA2/HB2/VB2 mur 2), agrège au jour. Garde-fou de plausibilité physique (retrait > 10 mm ⇒ exclu comme panne, pas comme pic). |
| `extraire_hr_t_reel.py` | Parcourt `Données HR-T.xlsx`, couche "milieu isolant", **par position** (pas de moyenne inter-position), exclut les relevés HR≥99,99 % (saturation) et les doublons sporadiques inter-capteurs (rafales < 30 échantillons, cf. section "anomalies" ci-dessous), agrège au jour. |
| `combiner_donnees_reelles.py` | Jointure sur la date en **intersection stricte** entre les deux sorties ci-dessus (retrait ET T/HR tous les deux mesurés), calcule le point de rosée réel (Magnus-Tetens, mêmes constantes que `ingestion_capteurs_bluetooth.py`), produit `donnees_reelles_finales.json`/`donnees_reelles_compact.json` (celui-ci embarqué tel quel dans l'artefact). |

Fenêtre retenue : 1<sup>er</sup> déc. 2025 → 26 mai 2026 — **99 jours**,
tous avec retrait ET T/HR mesurés, non consécutifs (la couverture des
fichiers `.dxd` était irrégulière avant le passage en cadence d'export
systématique).

**Historique** : 33 → 53 jours (24/07/2026) — la jointure ne rejette plus un
jour entier quand un seul des deux murs n'a pas de moyenne T/HR calculable
ce jour-là (ex. toutes les positions saturées) — ce mur affiche « — »
ponctuellement, l'autre mur et le retrait restent exploitables. Essai à 112
jours (28/07/2026, **abandonné le jour même**) : passage d'une intersection
à une **union** (retrait ET/OU T/HR), abandonné au profit du retour à
l'intersection stricte — mieux vaut n'afficher que des jours où retrait ET
T/HR sont tous deux de vraies mesures, plutôt que des jours partiels.

**53 → 99 jours (31/07/2026)** : un collègue a fait remarquer, à juste titre,
que 53 jours semblait faible vu la durée réelle de la campagne. Cause
trouvée : `extraire_retrait_reel.py` et `extraire_hr_t_reel.py` fixaient
tous deux `DEBUT`/`FIN` **en dur** à déc. 2025 → mars 2026 — une fenêtre
choisie tôt dans le projet et jamais mise à jour depuis, alors que la
collecte a continué.

**`DEBUT`/`FIN` supprimées, pas seulement élargies** : plutôt que de
re-choisir une nouvelle fenêtre fixe qui redeviendrait obsolète dans
quelques mois, les deux scripts traitent maintenant **tout ce qui est
disponible**, sans borne de dates. Rappel important (cf. plus haut) : ce
pipeline est un outillage **POC uniquement** — dans le produit final,
chaque fichier est ingéré à son arrivée, donc cette question de fenêtre à
maintenir ne se posera jamais. Supprimer les bornes évite juste de refaire
cette découverte à chaque future mise à jour du POC. Contrepartie
mineure : le temps d'extraction croît avec la taille de la campagne (357
fichiers retrait ≈ 6 min aujourd'hui) et chaque exécution donne un résultat
différent (plus grand) que la précédente — un "instantané toujours à jour"
plutôt qu'une fenêtre de recherche figée et reproductible, cohérent avec
l'usage (une campagne en cours, pas une étude rétrospective).

| Source | Jours distincts disponibles (sans borne) | Période |
|---|---|---|
| Retrait (`data_retrait/*.dxd`) | 167 jours (357 fichiers) | 21/11/2025 → 27/07/2026 |
| T/HR (`Données HR-T.xlsx`) | 177 jours (1396 mesures) | 01/12/2025 → 27/05/2026 |
| **Intersection stricte retenue** | **99 jours** | **01/12/2025 → 26/05/2026** |

L'intersection reste plafonnée par le T/HR (qui s'arrête fin mai 2026) : le
retrait disponible en juin-juillet 2026 n'apparaît donc pas dans l'abaque,
mais reste exploité pour repérer l'anomalie 3 ci-dessous. 78 jours de la
fenêtre T/HR complète (177 jours) n'ont aucun fichier `.dxd`
correspondant — reconfirmé (31/07/2026) que ce n'est pas un filtrage à tort
mais une vraie absence de mesure ; toujours écartés plutôt que comblés par
interpolation, conformément au principe déjà retenu (cf. paragraphe
précédent).

**Un correctif introduit pendant l'essai en union est resté, à raison** :
`filtrerHampel()` (artefact) traitait un retrait `null`/`undefined` comme un
pic à corriger (coercition JS silencieuse vers `0`) au lieu de l'ignorer.
Ce cas existe toujours au niveau d'un canal individuel (ex. HB2, HA2, HA1 —
cf. anomalies ci-dessous) — le correctif reste donc nécessaire et a été
conservé.

**Correction d'une affirmation précédente** : cette note indiquait que la
fenêtre "ne pouvait pas s'élargir plus" car les positions T/HR ne seraient
valides que jusqu'au 22 mars 2026 dans le classeur. Vérification directe du
classeur le 31/07/2026 : c'était inexact (ou périmé — le classeur source a
pu être mis à jour depuis) — des mesures T/HR exploitables existent bien
jusqu'au 27 mai 2026. Corrigé ici plutôt que laissé comme trace d'erreur.

### Anomalies réelles trouvées, documentées plutôt que masquées

1. **Capteur HB2 (mur 2), 4 mars 2026** : panne soutenue (-17,7 mm constant
   sur 100 % des échantillons de 2 fichiers) — catégoriquement différente
   d'un pic de vibration (section 17) : c'est un contrôle de plausibilité
   physique *inter-fichiers* qui l'a détectée, pas le filtre de Hampel
   (conçu pour des pics ponctuels *intra*-fichier). Point exclu.
2. **Capteur HA2 (mur 2), 30 avril → 18 mai 2026** (trouvé le 31/07/2026, à
   l'élargissement de la fenêtre) : panne soutenue similaire à HB2
   (-16,25 mm environ, constant), détectée et exclue par le même contrôle de
   plausibilité physique.
3. **Capteur HA1 (mur 1), à partir du 5 juin 2026, toujours en cours au
   27 juillet 2026** (trouvé le 31/07/2026, à la suppression des bornes
   `DEBUT`/`FIN`) : panne soutenue du même type (-10,80 mm environ,
   constant), sur la totalité des fichiers depuis cette date. **Hors de la
   période affichée dans l'abaque** (le T/HR s'arrête fin mai 2026, avant le
   début de cette panne) — n'a donc aucun effet sur le POC, mais mérite un
   signalement à l'équipe terrain : à la différence de HB2/HA2 (pannes
   ponctuelles, quelques semaines), celle-ci n'est toujours pas résolue au
   dernier fichier disponible.
4. **3 fichiers du 8 avril 2026** (trouvés le 31/07/2026) : contenaient par
   erreur des canaux de **diagnostic système** (CPU/mémoire/disque/réseau,
   ex. "MemTotal", "DiskFree") au lieu du seul retrait — sans lien avec un
   phénomène physique, exclus par le même contrôle de plausibilité (valeurs
   très hors de portée pour un retrait en mm). Cause probable : un groupe de
   canaux de supervision DeweSoft resté actif par erreur lors de ces
   mesures.
5. **Mur 1, à partir du 6 février 2026** : les 4 positions T/HR se mettent à
   reporter des valeurs strictement identiques **dans le classeur Excel
   source lui-même** (vérifié cellule par cellule, pas un artefact
   d'extraction), **en continu** (rafales de 336 à 567 échantillons) — le
   Mur 2 continue de varier normalement. Cause probable : panne d'acquisition
   ou bascule capteur/logger côté source, non investiguée plus avant.
   Conséquence pratique : le sélecteur de position n'apporte plus rien pour
   le Mur 1 après cette date, jusqu'à la coupure totale ci-dessous.
   **Gardé tel quel** (état soutenu, pas une valeur ponctuelle à corriger).
6. **Aucune T/HR exploitable (les deux murs), 25 mars → 26 mai 2026**
   (46 jours, trouvé le 31/07/2026 à l'élargissement de la fenêtre) :
   cellules T **et** HR réellement vides dans le classeur source pour
   toutes les positions des deux murs sur cette période — vérifié
   directement (pas un artefact de saturation ni de filtrage). Retrait
   gardé quand même pour ces jours (intersection stricte toujours respectée
   côté date), T/HR affiché « — ». Cause non investiguée (arrêt
   d'acquisition T/HR côté source, probablement).
7. **Doublons sporadiques inter-capteurs, ailleurs dans le classeur** :
   d'autres paires de capteurs sans lien physique (positions différentes,
   parfois des deux murs) rapportent occasionnellement des valeurs T *et*
   HR strictement identiques par courtes rafales (1 à ~15 échantillons
   épars dans tout le fichier) — une contamination croisée entre colonnes,
   pas un phénomène physique ni du bruit BLE. Repéré en creusant la question
   *"le filtre de Hampel marche-t-il aussi pour T/HR ?"* : la réponse est
   non — ce n'est pas le même type de bruit (voir ci-dessous) — mais
   l'analyse a mis au jour ce doublon. **Corrigé** (contrairement à
   l'anomalie 5) : `extraire_hr_t_reel.py` détecte les rafales de correspondance
   exacte entre chaque paire de positions et exclut celles de longueur
   **< 30 échantillons** (~3,75 j à 3h) comme doublon sporadique, tout en
   laissant intactes les rafales plus longues (l'anomalie 5, qui est un état
   soutenu et non une contamination ponctuelle — la marge entre les deux
   plages observées, ~15 vs ~238+ échantillons, est large).

**Pourquoi pas un filtre de Hampel pour T/HR ?** Vérifié empiriquement (39
capteurs, T et HR) : quasiment aucun pic ponctuel n'est présent, et les rares
écarts détectés sont soit les doublons ci-dessus (anomalie 3, exact, pas du
bruit), soit de mineurs sauts d'humidité *synchrones sur plusieurs capteurs
à la fois* (jusqu'à 12 positions, sur les deux murs, au même horodatage) —
une signature incompatible avec une interférence BLE indépendante par
capteur, plus probablement un bref événement environnemental réel ou un
artefact d'export groupé. Leur amplitude reste dans l'ordre de grandeur de
la variation normale (percentile 95-99 des écarts habituels), très loin des
~17000x observés sur le pic de calibration du retrait (section 17). Un
filtre de Hampel calé comme celui du retrait (k=8, fenêtre courte)
flaguerait de la variation normale comme aberrante sur un jeu de données
bien plus clairsemé (pas de 3h, pas le pas de quelques ms/s du retrait) —
**non retenu**. La détection de doublon (anomalie 3) cible le vrai problème
trouvé, avec un mécanisme différent (comparaison exacte inter-capteurs, pas
médiane/MAD glissante).

### Interface : sélecteur de position (données réelles uniquement)

Un sélecteur permet de choisir la position T/HR affichée (moyenne des 4
positions par défaut, ou une position précise) — le retrait garde son
sélecteur de canal existant (HA/VA/HB/VB), pas de doublon. Recalcul des
bornes d'axes, du cache du filtre de Hampel et du curseur temporel à chaque
changement de mur/position.

**Comparaison de plusieurs positions T/HR à la fois** : sur le même modèle
que la comparaison de canaux de retrait, une case à cocher fait apparaître
jusqu'à 4 courbes T/HR superposées (bas gauche/milieu, haut milieu/droite —
centre milieu partagé exclu, pas propre à un mur donc pas comparable aux 4
autres). Une position substitue sa température **et** son humidité en même
temps sur tous les axes concernés (X/Y/Z), puisqu'une position = un seul
capteur qui porte les deux grandeurs. **Mutuellement exclusive** avec la
comparaison de canaux de retrait (cocher l'une décoche l'autre) — mélanger
les deux aurait multiplié les courbes sans qu'aucune ne reste lisible.

### Couche de la paroi (28/07/2026)

Jusqu'ici, T/HR était figé sur la couche **« milieu isolant »** (choix fait
initialement, cf. début de cette section). Sur demande, la couche devient
elle aussi un axe de sélection — mur → position → **couche** — avec la même
logique de comparaison multiple que les positions.

**Mapping capteur → couche, reconstruit et vérifié le 28/07/2026** par
lecture directe du XML des graphiques du classeur (`xl/charts/chartN.xml` :
colonne de chaque série de graphique recoupée avec l'entête "Capteur" de la
feuille de données), confirmant à l'identique le mapping déjà établi. Chaque
position a 4 couches standards — Milieu carreau, Carreau/isolant, Milieu
isolant, Isolant/OSB (ordre physique extérieur → intérieur) — sauf
**Haut milieu** (les deux murs) et **Centre milieu partagé**, qui en ont une
5<sup>e</sup> : **Carreau/ext** (point de mesure côté extérieur).

`extraire_hr_t_reel.py` extrait maintenant les 39 capteurs (au lieu de 9) —
un par (mur, position, couche). Le détecteur de doublon sporadique (section
"anomalies" ci-dessus) tourne donc sur 741 paires au lieu de 36. **Bug trouvé
et corrigé pendant cette extension** : la condition de correspondance
comptait `None == None` (deux capteurs saturés/exclus **au même instant** —
un cas réel et fréquent, pas un doublon) comme une correspondance, gonflant
les exclusions de 431 à plusieurs milliers. Corrigé en exigeant que les 4
valeurs comparées (T et HR des deux capteurs) soient des nombres réels, pas
seulement égales — ramène les exclusions à un niveau plausible (~885 pour 39
capteurs, contre 431 pour 9). Ce bug affectait aussi, dans une moindre
mesure, la version à 9 capteurs déjà en production ; corrigé du même coup.

**Comparaison de couches, plafonnée à 4** (comme les canaux et les
positions) : Carreau/ext exclu de la comparaison multiple — aucune 5<sup>e</sup>
teinte n'a été trouvée qui passe la validation CVD all-pairs sans collision
avec les 4 couleurs déjà en place (essayé : violet, teal, olive, brun —
toutes échouent le plancher de vision normale ΔE<15 contre au moins une des
4 existantes). Carreau/ext reste sélectionnable seul via le menu déroulant.
La comparaison de couches ne s'active que pour une position précise (pas
« Moyenne ») et est **mutuellement exclusive** avec les deux autres
comparaisons (canaux de retrait, positions) — au total, trois modes de
comparaison qui s'excluent deux à deux.

### Statut : POC autonome, pas encore relié au pipeline applicatif

Comme pour la section 17, ceci vit uniquement dans l'artefact (fichier HTML
autonome, aucun composant frontend dans ce dépôt) et dans
`data_reel_compile/` (JSON pré-calculés, pas de lecture InfluxDB en direct).
Si ce mode "données réelles" doit un jour alimenter l'appli réelle, il faudra
un backend qui interroge InfluxDB avec la même logique de jointure
retrait/T-HR/position — pas seulement rejouer les scripts Python actuels.

### Variante démo — agrégation par tranche de 3h (31/07/2026)

Pour explorer si un pas plus fin que le jour révèle des cycles jour/nuit
utiles à une démo (question posée après avoir chiffré l'écart de cadence
retrait 10 Hz / T/HR 3h — cf. discussion dédiée), une **copie dédiée** de
tout le pipeline a été créée, **sans toucher aux scripts/artefact de
référence** (qui restent l'agrégation au jour, section 18 ci-dessus) :

| Référence (jour) | Copie démo (3h) |
|---|---|
| `extraire_retrait_reel.py` | `extraire_retrait_reel_3h.py` |
| `extraire_hr_t_reel.py` | `extraire_hr_t_reel_3h.py` |
| `combiner_donnees_reelles.py` | `combiner_donnees_reelles_3h.py` |
| `retrait_reel.json`, `hr_t_reel.json` | `retrait_reel_3h.json`, `hr_t_reel_3h.json` |
| `donnees_reelles_finales/compact.json` | `donnees_reelles_finales/compact_3h.json` |
| `abaque-3d-hygrothermique.html` | `abaque-3d-hygrothermique-3h.html` (artefact séparé, URL distincte) |

**Résultat** : 642 tranches de 3h (contre 99 jours pour la référence),
1er déc. 2025 → 26 mai 2026.

**Particularité technique** : le découpage par tranche de 3h a besoin de
l'horodatage RÉEL de chaque échantillon 10 Hz, pas seulement de la date du
fichier (déduite du nom dans la version au jour) — les fichiers `.dxd` ne
démarrent pas sur une frontière de tranche (ex. 03h53, 15h53), et l'écart
entre l'heure du nom de fichier et l'heure interne réelle peut atteindre
~1h (vérifié : un fichier nommé "081014" démarre en réalité à 07h10).
`extraire_retrait_reel_3h.py` utilise donc `DWIGetMeasurementInfo`
(`start_store_time`) comme le fait `ingestion_dewesoft_dxd.py`, puis
calcule les bornes d'index de chaque tranche par arithmétique (échantillons
uniformément espacés à 10 Hz) plutôt que de tester chaque échantillon un
par un — un test par échantillon aurait été beaucoup trop lent en Python
pur (~432 000 échantillons/fichier/canal). Logique validée sur un fichier
réel avant de lancer le lot complet.

**Ajustement nécessaire côté abaque** : la fenêtre du filtre de Hampel
(demi-fenêtre, en nombre de points) a été portée de 10 à 80 — à 8 tranches
de 3h par jour, 80 tranches couvrent la même portée calendaire (~10 jours)
que 10 points en pas journalier ; sans cet ajustement, le lissage aurait
porté sur une fenêtre 8× plus courte en temps réel.

**Coût** : fichier artefact ~2,9 Mo (contre ~575 Ko pour la référence) —
encore raisonnable pour une démo statique, mais nettement plus lourd. Pas
de plan pour remplacer la référence au jour par cette variante ; les deux
coexistent, la variante 3h n'étant qu'une exploration.

## 19. Registre d'étiquetage mur/couche/position (produit final, 28/07/2026)

Objectif : que l'application puisse, pour un mur donné, retrouver la liste
des capteurs qui y sont embarqués — **indépendamment** du POC de l'abaque
(section 18), qui utilise sa propre logique figée dérivée du fichier Excel de
simulation. Ce fichier Excel **ne sert qu'au POC** : aucun lien n'existe (et
ne doit exister) entre lui et les registres décrits ici, qui visent le
produit final.

### Capteurs HR/T — extension de `capteurs.json` (section 3)

Les capteurs HR/T embarqués dans une paroi sont, dans le produit final, des
capteurs **Blue Maestro BLE** comme les autres — pas un système séparé. Le
mur/couche/position s'ajoutent donc comme champs supplémentaires,
optionnels, sur les entrées existantes de `capteurs.json` (détail en
section 3) plutôt que dans un fichier à part.

### Capteurs de retrait — nouveau fichier `capteurs_retrait.json`

Contrairement au BLE, un capteur de retrait est **filaire**, identifié par
son **nom de canal DeweSoft** (ex. `HA1`), pas par une adresse MAC — pas
d'identifiant matériel unique fourni par le fabricant, pas de découverte
BLE. Un registre séparé est donc justifié : les champs BLE (`mac`,
`lint_configure`, `lint_gatt_absent`...) n'ont pas de sens pour un canal
filaire.

Schéma (mêmes principes que `capteurs.json` — `_schema` auto-documenté,
champs libres/non contraints) :

```json
{
  "_schema": {
    "_description": "Mapping nom de canal DeweSoft (fichiers .dxd, data_retrait/) → étiquetage mur/couche/position pour les capteurs de retrait filaires. Pas d'adresse MAC ni de découverte BLE : les canaux sont fixés par le câblage du rig DeweSoft ; auto-enregistrés vides à la première lecture, complétés ensuite par l'utilisateur.",
    "canal": "Nom du canal tel qu'il apparaît dans les fichiers .dxd (ex. 'HA1'). LECTURE SEULE — doit correspondre à la clé parente.",
    "nom_mur": "Nom du mur/paroi sur lequel le capteur est monté. Éditable ; vide si non applicable/inconnu.",
    "nom_couche": "Couche de la paroi, si applicable (rare pour un capteur de surface). Éditable ; vide par défaut.",
    "position": "Position du capteur sur le mur (ex. 'Bas gauche'), si connue. Éditable ; vide par défaut.",
    "categorie R&D": "Catégorie R&D associée (ex. 'Retrait'). Libre.",
    "prestation": "Référence de prestation associée. Libre.",
    "ingestion": "true = ce canal est pris en compte par ingestion_dewesoft_dxd.py ; false (défaut à l'auto-enregistrement) = exclu."
  }
}
```

Pas de champ `partage_entre_murs` (tranché le 28/07/2026 : n'a pas de sens
pour un capteur BLE, et pour le retrait le même besoin serait plutôt
couvert par le futur usage du champ `categorie R&D`/une valeur `nom_mur`
laissée vide plutôt qu'un booléen dédié).

**Auto-enregistrement, symétrique du BLE** : à la lecture de chaque fichier
`.dxd`, pour tout `canal_nom` absent du registre, `ingestion_dewesoft_dxd.py`
ajoute automatiquement une entrée vide (`ingestion: false`) — aucun canal
n'est ingéré silencieusement sans laisser de trace à étiqueter. Le fichier
part vide (pas de pré-remplissage manuel des 8 canaux déjà connus) : le
premier fichier `.dxd` traité les fait apparaître de lui-même.

**Rechargement à chaud** : même mécanisme que `capteurs.json` côté BLE
(comparaison de la date de modification du fichier à chaque tour de la
boucle de surveillance) — une modification de `capteurs_retrait.json` par
l'utilisateur est prise en compte sans redémarrer le script.

**Risque identifié et traité — collision de noms de canal** : les 8 canaux
actuels (HA1/VA1/HB1/VB1 = Mur 1, HA2/VA2/HB2/VB2 = Mur 2) vivent **tous
dans le même fichier `.dxd`, en même temps** (vérifié : un fichier réel
contient bien les 8 canaux des deux murs simultanément) — il n'y a **aucune
séparation par dossier/fichier** entre murs, tout repose sur le nom du
canal étant globalement unique. Le champ `description` du canal (qui aurait
pu porter une info complémentaire) est vide dans les fichiers actuels — pas
de filet de sécurité disponible côté métadonnées DeweSoft. Si un jour deux
murs partageaient par erreur un même nom de canal (ex. deux "VA1"
distincts), rien ne permettrait de les distinguer par les données seules.
Traitement : convention de nommage globalement unique à maintenir côté
configuration DeweSoft (déjà le cas aujourd'hui via le chiffre final) +
garde-fou logiciel (ci-dessous) qui détecte et signale la collision au lieu
de la laisser corrompre silencieusement une étiquette.

**Garde-fou collision** : si un fichier `.dxd` contient deux canaux de même
nom, `ingestion_dewesoft_dxd.py` ne les traite jamais comme un seul —
l'anomalie est signalée par un `print()` (visible en direct) **et** publiée
comme point InfluxDB dédié (mesure `alertes_ingestion`, tag
`type: "collision_canal"`, `canal_nom`, `fichier_source`) via le chemin
MQTT → Kafka → InfluxDB déjà en place. Motivation du choix InfluxDB plutôt
que le seul log console : aucun script du pipeline n'a de journalisation
persistante aujourd'hui (tout part sur `stdout`, perdu si personne ne
regarde au bon moment) — publier l'anomalie comme donnée la rend
persistante, horodatée, interrogeable en Flux, et exploitable dans Grafana
(déjà déployé et validé dans ce projet) sans outil supplémentaire.

**Où ces erreurs sont monitorées** : dans Grafana, pas dans un nouvel onglet
de l'application utilisateur — cohérent avec la décision de garder
l'interface abaque réservée au monitoring/filtrage/visualisation des
données par l'utilisateur final (cf. section 18), pas à la gestion
technique des capteurs/registres.

**Implémenté (29/07/2026)** : `capteurs_retrait.json` créé (vide, `_schema`
seul) ; `ingestion_dewesoft_dxd.py` complété avec le rechargement à chaud,
l'auto-enregistrement (`enregistrer_canal_si_inconnu`), la porte `ingestion`
(un canal `false` — défaut à l'auto-enregistrement — n'est tout simplement
pas publié), le garde-fou collision (`print` + publication sur le nouveau
topic MQTT `frd/dewesoft/alertes`), et l'enrichissement du payload
`mesures_dewesoft` avec `nom_mur`/`nom_couche`/`position`/`categorie R&D`.
`bridge_mqtt_to_kafka.py` relaie ce nouveau topic vers
`murmetric.{tenant}.dewesoft.alertes` ; `kafka_consumer_influx.py` y ajoute
un consommateur dédié (`construire_point_alerte`, mesure `alertes_ingestion`)
et reçoit les mêmes tags mur/couche/position sur `mesures_dewesoft`. Au
passage, `construire_point_registre` dans `kafka_consumer_influx.py` (chemin
Kafka, celui retenu comme principal — cf. Points ouverts) a été corrigé : il
lui manquait les tags `nom_mur`/`nom_couche`/`position` déjà présents côté
`bridge_mqtt_to_influx.py`, et il portait encore l'ancienne boucle
`latitude`/`longitude`/`altitude_m` (champs supprimés de `capteurs.json`).

## 20. Courbes agrégées — somme et moyenne entre séries (conception — non implémenté, 29/07/2026)

Objectif : en plus du mode **comparaison** déjà prévu (section 17/18 —
plusieurs courbes distinctes affichées côte à côte, plafonné à 4), permettre
d'afficher une **courbe unique dérivée**, calculée point par point à partir
de plusieurs séries sélectionnées par l'utilisateur, selon **deux opérations
au choix** :

- **Somme arithmétique** (ex. `VA1(t) + VA2(t)`) ;
- **Moyenne arithmétique** (ex. `moyenne(VA1(t), VA2(t))`).

**Champs concernés — les trois familles de données du projet** :
- **Canaux de retrait** (DeweSoft) — ex. `VA1 + VA2`, `HA1 + HA2`.
- **Teneur en eau** (section 16) — ex. somme/moyenne entre couches saisies
  manuellement.
- **HR et T** (capteurs BLE) — même possibilité prévue, entre positions ou
  entre couches selon l'axe choisi par l'utilisateur.

Cette courbe agrégée est **distincte** du mode comparaison : ce n'est pas
plusieurs courbes superposées, mais une seule courbe calculée, qui vient
s'ajouter aux choix d'affichage de l'utilisateur.

**Alignement temporel — deux cas différents** :
- **Retrait et HR/T** : échantillonnage dense et régulier (1 mesure/s pour le
  retrait, cadence propre au capteur BLE pour HR/T) — les séries à combiner
  partagent déjà le même axe temporel, la somme/moyenne point par point est
  directe (`aggregateWindow` équivalent, sans jointure particulière).
- **Teneur en eau** : saisies manuelles éparses (section 16) — les couches à
  combiner n'ont pas forcément été saisies au même instant. Combiner deux
  couches demande donc la même logique de **jointure "au plus proche"** déjà
  prévue en section 16 pour croiser teneur en eau et T/HR, appliquée cette
  fois entre couches de teneur en eau plutôt qu'entre teneur en eau et T/HR.

Non implémenté à ce stade — conception uniquement, à construire au moment de
développer l'interface applicative réelle (l'abaque 3D reste un POC
autonome, cf. section 18).

## 21. Résolution mur/couche/position → capteur(s) réel(s) (analyse, 30/07/2026)

Question posée : quand l'utilisateur choisit un mur, une couche et une
position dans l'interface, cela filtre-t-il une liste de capteurs
correspondants ?

**Dans le POC (section 18)** : non, il ne s'agit pas d'un filtrage. La
structure de données dérivée du classeur Excel garantit une correspondance
**1:1:1:1** — chaque combinaison (mur, position, couche) pointe vers
exactement un capteur (`d.positions[position].couches[couche]`), par
construction (4 positions × 5 couches par mur, une colonne par capteur dans
le classeur source). Il n'y a donc jamais 0 ni plusieurs candidats à
départager dans le POC.

**Dans le produit final, la situation est différente.** `capteurs.json` et
`capteurs_retrait.json` (section 19) utilisent des champs
`nom_mur`/`nom_couche`/`position` **libres et non contraints** — décision
volontaire pour ne pas figer par avance le nombre de murs/couches/positions
du système. Rien n'empêche alors, en pratique :
- **Zéro capteur correspondant** — étiquette pas encore renseignée, faute de
  frappe (ex. "Haut Milieu" vs "haut milieu" vs "Haut-Milieu").
- **Plusieurs capteurs correspondants** — deux capteurs étiquetés par erreur
  avec la même combinaison (mur, couche, position), ou redondance
  volontaire (deux capteurs au même emplacement nominal pour validation
  croisée).

Sans traitement particulier, ces deux cas se traduiraient par un graphique
vide (0 capteur) ou par un choix silencieux arbitraire du premier capteur
trouvé (plusieurs capteurs) — dans les deux cas, l'utilisateur ne
comprendrait pas pourquoi la courbe est absente ou ne correspond pas à ce
qu'il attend.

**Recommandation (non implémentée)** : plutôt qu'une liste de capteurs à
parcourir dans l'interface (complexité ajoutée pour un cas qui, avec un bon
étiquetage, ne montre presque toujours qu'un seul élément), un **indicateur
inline** suffirait :
- Cas normal (exactement un capteur résolu) : afficher son nom (ou MAC)
  directement dans la légende/l'infobulle de la courbe — confirme quel
  capteur physique alimente les données affichées.
- Cas anormal (0 ou plusieurs capteurs résolus pour la combinaison choisie) :
  alerte explicite plutôt qu'un graphique vide ou un choix arbitraire.

Cette approche rejoint la philosophie déjà retenue pour les collisions de
canaux de retrait (section 19, mesure `alertes_ingestion`) : ne jamais
échouer silencieusement, signaler l'anomalie plutôt que de la masquer ou de
la deviner. Non implémenté à ce stade — pertinent pour l'interface
applicative réelle, pas pour le POC (dont la structure de données exclut
structurellement ce problème).

## 22. Arrière-plan personnalisable de la zone d'affichage (POC, 30/07/2026)

L'abaque 3D permet désormais de choisir la couleur de l'arrière-plan de la
zone d'affichage (`.stage-inner`), via un sélecteur de couleur unique dans
le panneau latéral, à côté d'un lien "Réinitialiser". Même principe que les
couleurs personnalisables déjà en place pour les canaux/positions/couches
(section 19/20) : le sélecteur est pré-rempli avec la couleur du thème
actif, donc aucun changement visuel tant que l'utilisateur n'y touche pas.

**Simplification assumée** : l'arrière-plan par défaut est un dégradé radial
entre deux teintes (`--stage-bg`/`--stage-bg-2`, variables selon le thème
clair/sombre). La personnalisation le remplace par une **couleur unie**
plutôt que d'exposer les deux teintes du dégradé — plus simple à piloter
avec un seul sélecteur, cohérent avec les contrôles de couleur existants.

**Point ouvert, volontairement non traité dans le POC** : aucun ajustement
automatique du contraste texte/grille n'accompagne ce changement de
couleur. Le texte (`--stage-text`/`--stage-text-dim`) et la grille
(`--stage-grid`/`--stage-grid-strong`) restent aux teintes claires pensées
pour un fond sombre — si l'utilisateur choisit une couleur de fond claire,
la lisibilité peut se dégrader (texte clair sur fond clair). Assumé pour le
POC (complexité non justifiée à ce stade) mais **prévu pour le produit
final** : calculer automatiquement la luminance de la couleur choisie et
basculer le texte/la grille vers une teinte sombre ou claire selon le
contraste obtenu (même logique que la validation CVD/contraste déjà
appliquée ailleurs dans le projet pour les palettes de courbes), plutôt que
de laisser l'utilisateur produire par erreur un graphique illisible.

## 23. Axes gradués — vue 2D uniquement (POC, 30/07/2026)

Chaque axe actif (X/Y/Z) affiche des graduations intermédiaires (petite
marque + valeur), mais **seulement en vue 2D** (un axe mis sur "Aucun",
projection orthographique à plat). En vue 3D (rotation libre), aucune
graduation intermédiaire — des graduations sur 2-3 axes vus sous un angle
quelconque surchargeraient vite la vue et se chevaucheraient avec les
points de données.

**Minimum et maximum exacts** (30/07/2026) : dans les deux modes, chaque
axe affiche aussi son minimum et son maximum **exacts** (pas seulement des
valeurs rondes proches) — à l'origine et à l'extrémité du trait. En 2D,
ça complète les graduations arrondies (qui tombent rarement pile sur les
bornes réelles) ; en 3D, c'est la seule info numérique sur l'axe.

**Pourquoi limité au 2D** : `projectSmart()` est purement orthographique en
2D (`yaw`/`pitch` figés à 0, `nz` figé à 0) — interpoler linéairement entre
les deux extrémités de l'axe à l'écran correspond alors exactement à
interpoler la valeur normalisée sous-jacente, sans distorsion de
perspective à corriger. Ça ne serait pas vrai en 3D (la perspective courbe
l'espacement des graduations).

**Valeurs des graduations — algorithme "à valeurs rondes"** (fonction
`calculerGraduations()`, méthode de Heckbert utilisée par D3/Excel) : plutôt
que diviser l'écart min/max réel en parts égales (ce qui donnerait des
virgules arbitraires, ex. 18.3/19.9/21.5), l'algorithme choisit un **pas
rond** (1, 2, 5, 10, 20, 25, 50…) adapté à l'échelle de la grandeur
affichée, puis place les graduations sur les multiples de ce pas contenus
dans l'écart affiché. Conséquences :
- Le nombre réel de graduations varie selon l'écart (visé ~8, mais peut
  descendre à 3 sur un écart étroit comme l'humidité, ou monter à 10 sur un
  écart large) — jamais figé à une valeur fixe.
- Les graduations restent **dans les bornes exactement tracées** de l'axe
  (pas d'extension du domaine affiché aux prochaines valeurs rondes,
  contrairement à l'affichage "large" par défaut d'Excel/matplotlib) — pour
  ne pas décaler l'échelle des points de données déjà calculée par
  `recalculerRanges()`.
- Le nombre de décimales affichées s'adapte automatiquement au pas choisi
  (ex. pas=0.1 → 1 décimale, pas=5 → 0 décimale) plutôt que d'utiliser
  systématiquement le nombre de décimales fixe de la grandeur (`AXES[k].decimals`,
  toujours utilisé en vue 3D pour le seul minimum affiché).

**Chevauchement étiquette exacte / graduation ronde (corrigé le
31/07/2026)** : quand une graduation ronde tombe très près d'une borne
exacte (ex. borne max = 101, graduation la plus proche = 100), les deux
étiquettes se dessinaient au même endroit à l'écran et fusionnaient en un
texte illisible (ex. "100101"). Corrigé en calculant la distance en pixels
entre chaque graduation et les deux extrémités de l'axe (`f * len` et
`(1 - f) * len`, où `len` est la longueur écran du trait) : en dessous d'un
seuil de 18px, l'étiquette de la graduation est simplement omise (le trait
de graduation reste dessiné) — seule l'étiquette exacte, plus informative,
reste affichée à cet endroit.

## 24. Lecture de valeur par projection (POC, 30/07/2026)

Objectif : permettre de lire, à partir d'une valeur cible sur un axe (ex.
Température = 20°C), la ou les valeurs correspondantes sur l'autre axe —
comme on projetterait à la règle depuis un axe jusqu'à la courbe, puis
jusqu'à l'autre axe. Réservé à la **vue 2D** (un axe sur "Aucun") pour la
même raison que les graduations (section 23) : les lignes de projection
perdraient leur sens visuel dans une vue 3D qui peut tourner.

### Pourquoi une recherche, pas une fonction mathématique

Les données ne sont pas une fonction simple (une Température ne donne pas
*une* Humidité) — c'est une **série temporelle** : chaque jour a son propre
couple de valeurs. La courbe peut repasser plusieurs fois par une même
valeur cible à des dates différentes, avec une valeur différente sur
l'autre axe à chaque fois. La fonctionnalité cherche donc, parmi les points
**réellement affichés** (période en cours, courbe choisie), celui ou ceux
dont la valeur sur l'axe cible est la plus proche de la cible saisie —
**tous** les points trouvés dans une tolérance (2 % de l'étendue de l'axe)
sont surlignés et étiquetés, jamais un seul choisi arbitrairement qui
cacherait une ambiguïté réelle des données.

### Workflow

1. Deux champs numériques apparaissent (un par axe actif), avec le nom et
   l'unité de l'axe correspondant. Taper une valeur dans l'un désigne cet
   axe comme cible et vide l'autre champ (jamais deux points de départ
   ambigus à la fois).
2. **Si une seule courbe est affichée** (pas de comparaison canaux/positions/
   couches active) : la recherche porte dessus automatiquement.
3. **Si plusieurs courbes sont affichées** (comparaison active) : un clic
   sur une entrée de la **légende** du graphique désigne la courbe sur
   laquelle chercher (surlignée dans la légende) — identifiée par son
   triplet `(channelKey, positionKey, coucheKey)`, pas par son texte, pour
   rester robuste si les libellés changent. Sans ce choix, un message
   invite à cliquer une courbe.
4. Résultat, dessiné sur le graphique : un marqueur sur l'axe cible à la
   valeur saisie, une ligne pointillée jusqu'à chaque point trouvé sur la
   courbe (cercle blanc, même style que le survol), puis une seconde ligne
   pointillée projetant chaque point vers l'autre axe, avec la valeur lue
   et la date du point en étiquette.
5. Un bouton "Effacer la projection" (ou vider les champs) retire
   marqueurs et pointillés.

### Simplification géométrique exploitée

En vue 2D, les deux axes actifs sont toujours parfaitement horizontal et
vertical à l'écran (projection orthographique, `yaw`/`pitch` figés à 0,
origine commune) — la projection perpendiculaire d'un point vers l'axe
"autre" se réduit donc à maintenir la coordonnée fixe de cet axe (x fixe
s'il est vertical, y fixe s'il est horizontal), sans calcul géométrique
complexe.

## 25. Graphiques compagnons valeur/temps (POC, vue 2D uniquement, 31/07/2026)

### Demande initiale et pourquoi elle a été reformulée

Un utilisateur a souhaité qu'un « second repère » avec le temps en abscisse
**et** en ordonnée entoure le repère X/Y en vue 2D, comme un cadre extérieur.
Analyse : géométriquement, cela n'a pas de sens tel quel — une trajectoire
dans le plan (X, Y) n'est pas une fonction monotone du temps (la courbe
repasse par les mêmes zones à des dates différentes), donc « le temps » ne
peut pas être à la fois l'axe horizontal ET l'axe vertical d'un même cadre
sans se contredire. L'équivalent réellement implémentable est deux **petits
graphiques compagnons séparés**, où le temps est authentiquement l'axe
horizontal : un pour la grandeur de l'axe X du graphique principal, un pour
celle de l'axe Y — chacun affichant "valeur en fonction du temps" (une vraie
fonction, contrairement au repère principal). Alternative proposée puis
validée par l'utilisateur.

### Portée du MVP

- Réservé à la **vue 2D** (mêmes raisons que sections 23/24) : en 2D, les
  deux graphiques compagnons correspondent exactement aux deux axes actifs
  (`getAxisMode().activeSlots`), quel que soit celui des trois (X/Y/Z) mis
  sur "Aucun".
- **Courbes multiples (précisé le 31/07/2026)** : si une comparaison
  (canaux de retrait, positions ou couches T/HR) est active sur le
  graphique principal, le compagnon de l'axe concerné affiche lui aussi une
  ligne par courbe active — mêmes couleur et style de trait (plein/tirets/
  pointillés) que la légende du graphique principal, réutilisant
  directement le tableau `legendChannels` et la fonction `axisValue()`
  construits dans `render()` (passés en argument à `dessinerCompagnons()`
  plutôt que remontés en variables globales). L'autre compagnon, si son axe
  n'est concerné par aucune comparaison active, garde la courbe moyenne par
  défaut. Filtrage par axe : les entrées avec `channelKey` s'appliquent au
  compagnon "Retrait", celles avec `positionKey` (positions ou couches) aux
  compagnons température/humidité/rosée/teneur en eau.
- Repère vertical synchronisé avec le survol du graphique principal
  (`hoveredIndex`), pour relier visuellement un point survolé à sa position
  dans le temps.
- Graduations de valeur "à valeurs rondes" en réutilisant
  `calculerGraduations()` (section 23), à 4 graduations cibles (au lieu de 8)
  vu la hauteur réduite (140px).
- Case à cocher "Graphiques compagnons" (masqués par défaut) : active/désactive
  l'affichage sans dépendre uniquement du mode 2D/3D.
- **Positionnement (deux tentatives, corrigé le 31/07/2026)** : première
  tentative — `#companion-wrap` enfant direct de `.layout` (grille CSS
  `258px 1fr`) avec `grid-column: 2` forcé, pour rester dans la colonne du
  graphique. **Insuffisant** : une grille CSS partage la **hauteur de
  ligne** entre toutes ses colonnes — comme `.rail` (panneau latéral, avec
  beaucoup de champs) est plus haut que `.stage-wrap` (le graphique), la
  ligne entière s'étire à la hauteur de `.rail`, et un 2ᵉ élément en colonne
  2 n'apparaît qu'après la fin de cette ligne, donc après la fin du panneau
  latéral, pas juste après le graphique — d'où le grand espace vide constaté
  à l'usage. **Correction définitive** : `.stage-wrap` et `#companion-wrap`
  sont désormais regroupés dans un conteneur `.stage-col`
  (`display: flex; flex-direction: column; gap: 16px;`), qui devient le seul
  enfant de la colonne 2 de `.layout` — les deux empilent alors l'un sous
  l'autre indépendamment de la hauteur de `.rail`, puisqu'ils partagent le
  même flux flex plutôt que la même ligne de grille.

### Conception « facile à retirer »

Contrainte explicite de l'utilisateur : si la fonctionnalité ne convient
pas, elle doit pouvoir être supprimée proprement. Toutes les additions sont
regroupées et marquées d'un commentaire "COMPAGNONS", en cinq endroits
précis dans `abaque-3d-hygrothermique.html` :
1. Bloc CSS `.companion-wrap`, `.companion-wrap.visible`, `.companion-chart`
   et ses enfants (le conteneur `.stage-col` n'est pas marqué "COMPAGNONS" —
   si la fonctionnalité est retirée, il suffit de remettre `.stage-wrap` en
   enfant direct de `.layout` et de supprimer `.stage-col`).
2. Bloc HTML `#companion-toggle-group` (case à cocher, juste après
   `#proj-group`).
3. Bloc HTML `#companion-wrap` (les deux `<canvas>` et leurs libellés,
   dans `.stage-col`, juste après `.stage-wrap`).
4. Bloc JS (fonctions `updateCompanionVisibility()`,
   `dessinerCompagnonUnique()`, `dessinerCompagnons()`, juste avant
   `render()`) + l'appel `updateCompanionVisibility()` ajouté dans
   `onAxisChange()`.
5. L'unique ligne d'appel `dessinerCompagnons();` en fin de `render()`.

Supprimer ces cinq blocs retire la fonctionnalité sans laisser de résidu.

### Statut

Implémenté à l'identique dans l'abaque de référence (jour) et dans la
variante démo 3h (`abaque-3d-hygrothermique-3h.html`, section 18) — mêmes
cinq blocs "COMPAGNONS", `getSeriesFiltrees()` de la variante 3h utilisant
déjà sa propre demi-fenêtre Hampel (80, cf. section 18) sans adaptation
supplémentaire nécessaire côté compagnons.

## 26. Cadre temporel englobant (POC, vue 2D uniquement, 03/08/2026)

### Origine de la demande — deux reformulations

Fait suite à la demande initiale du "second repère englobant" (section 25,
temps en abscisse ET en ordonnée d'un cadre extérieur). Deux précisions
successives de l'utilisateur ont fait évoluer la conception :
1. Le besoin n'est **pas géométrique** mais lié à l'**impression/capture
   d'écran** — l'infobulle de survol (qui affiche la date exacte d'un point)
   disparaît sur une image figée, donc plus aucun moyen de savoir "à quel
   moment" correspond un point une fois le graphique capturé pour un rapport.
   (Une première implémentation avait alors peint quelques dates directement
   sur la courbe — remplacée par ce qui suit.)
2. L'utilisateur a ensuite demandé le cadre englobant **littéral** malgré
   tout, avec le temps sur les 2 côtés (x et y) — pas de simples étiquettes
   sur la courbe.

### Solution implémentée : projection à deux bords

Pour concilier la demande littérale avec la contrainte géométrique déjà
identifiée (la position X ou Y d'un point n'est pas une fonction bijective
du temps — une trajectoire peut repasser par la même zone à des dates
différentes, donc un axe de temps continu unique n'a pas de sens) :

- Un **second repère** est tracé, parallèle aux 2 axes de valeur actifs,
  décalé vers l'extérieur (26px, du côté opposé au centre du graphique —
  même heuristique que `etiquetteBorne()`, section 23).
- Pour quelques points choisis le long de la trajectoire par défaut (6,
  indices régulièrement espacés), **deux lignes de projection** partent du
  point vers ce cadre extérieur (une par côté) — même principe que la
  lecture par projection (section 24) — avec une graduation + la date à
  l'endroit où chaque ligne l'atteint.
- Ce n'est **pas un axe continu unique** : chaque point projeté porte sa
  **propre** date ; deux points différents peuvent tout à fait projeter au
  même endroit sur un des deux côtés avec des dates différentes (la
  contradiction géométrique n'est pas résolue, seulement rendue non
  trompeuse — chaque étiquette reste individuellement correcte).
- Techniquement : `axisEcran[slot]` porte désormais aussi `perpX`/`perpY`
  (calculés dans la boucle de tracé des axes) pour permettre de reconstruire
  la ligne parallèle en dehors de `render()` ; le point projeté sur cette
  ligne pour un point `p` de la courbe est simplement `p` translaté du même
  vecteur de décalage que la ligne (puisqu'elle lui est parallèle).

Portée, pour rester simple et lisible — inchangée par rapport à la première
version :
- **Vue 2D uniquement** (mêmes raisons que sections 23/24/25).
- **Trajectoire par défaut uniquement** (pas de comparaison canaux/positions/
  couches active).
- **Case à cocher dédiée** ("Cadre temporel englobant"), décochée par
  défaut — à activer spécifiquement avant une capture d'écran pour un
  rapport.

Bloc isolé exprès (case à cocher `#reperes-temps-toggle-group`, fonction
`dessinerCadreTempsEnglobant()` juste avant `render()`, appel dans
`render()`, `perpX`/`perpY` ajoutés à `axisEcran`) : supprimer ces quatre
endroits retire la fonctionnalité sans résidu (le retrait de `perpX`/`perpY`
d'`axisEcran` est sans risque, rien d'autre ne les lit). Implémenté à
l'identique dans les deux fichiers (jour et démo 3h).

### Numérotation plutôt que dates en toutes lettres, et retrait des marqueurs sur la courbe (03/08/2026)

Deux ajustements demandés après un premier essai :
- **Plus aucun marqueur ni texte sur la trajectoire elle-même** — seule la
  ligne de projection (fine, semi-transparente) part du point vers le cadre ;
  le point n'est plus mis en évidence sur la courbe (le petit disque blanc
  a été retiré).
- **Repères numérotés (1 à 6) sur le cadre**, plutôt que la date écrite en
  toutes lettres à chaque graduation — plus compact, surtout quand les deux
  projections d'un même point tombent près l'une de l'autre sur un bord.
  La correspondance numéro → date exacte est reportée dans une **légende
  permanente** en haut à droite du graphique (`#reperes-temps-legend`,
  élément HTML superposé au canvas comme `#hud-title`/`#legend-time`), afin
  de ne pas perdre l'information : c'est tout l'intérêt de cette
  fonctionnalité (rester lisible sur une image figée) de ne pas la renvoyer
  derrière un survol.

### Correction : graduations RÉGULIÈRES par indice temporel, pas dérivées de la courbe (03/08/2026)

La version ci-dessus plaçait encore les 6 repères en projetant des points
choisis **sur la trajectoire** vers le cadre (lignes de projection depuis la
courbe) — l'utilisateur a précisé que ce n'était pas la demande : les 2
côtés du cadre sont des **axes de temps à part entière**, donc leurs
graduations doivent être **régulièrement espacées par indice temporel**
(comme un axe de valeur normal), pas positionnées selon où la courbe passe.

Corrigé : chaque graduation `k` (0 à 5) est placée à la fraction
`k / (NB_REPERES_TEMPS - 1)` de la longueur du côté du cadre — exactement
comme une graduation de valeur classique interpole entre `origin` et `far`
— au lieu d'être dérivée de la position d'un point de la courbe. Les 2
lignes de projection depuis la courbe et le petit marqueur blanc ont
disparu : le cadre est désormais un **axe de temps indépendant**, sans lien
géométrique avec la courbe elle-même. Ce découplage règle aussi, de fait, la
contradiction géométrique identifiée section 25 (position non bijective au
temps) : un axe gradué par indice temporel, sans référence à la position de
la courbe, n'a lui aucune ambiguïté — chaque graduation correspond à un seul
instant, par construction. `dessinerCadreTempsEnglobant()` prend désormais
`cutoffN` (nombre de points affichés) au lieu du tableau `pts` de la
trajectoire.

### Deuxième correction : réutiliser les graduations de l'axe "Temps" plutôt qu'une numérotation maison (03/08/2026)

Même la version "graduée par indice" ci-dessus restait un système maison
(numéros 1 à 6 + légende séparée #reperes-temps-legend). L'utilisateur a
précisé, capture à l'appui, que les graduations du cadre doivent ressembler
**exactement** à celles de l'axe "Temps" déjà existant (bornes exactes +
valeurs rondes, ex. "-115 h", "0 h", "1000 h", "2038 h" sur la capture) —
et que l'utilisateur doit pouvoir changer l'unité affichée (heure/jour/
semaine/mois) **exactement comme pour n'importe quel axe assigné "Temps"**.

Corrigé en réutilisant directement l'infrastructure existante plutôt qu'un
système séparé :
- `ranges.t` est **toujours calculé** par `recalculerRanges()` (fait partie
  de `AXIS_ORDER`), que "Temps" soit ou non assigné à un axe visible — donc
  disponible pour graduer le cadre même quand aucun axe X/Y n'est "Temps".
- `dessinerCadreTempsEnglobant()` appelle `calculerGraduations(ranges.t.lo,
  ranges.t.hi, 8)` et `formatTemps()`, exactement comme la boucle de tracé
  des axes (section 23) — même seuil anti-chevauchement (18px) que le
  correctif de cette section.
- **Plus de numérotation ni de légende séparée** : les graduations
  affichent directement la valeur de temps formatée (ex. "1000 h"), donc
  elles se suffisent à elles-mêmes — `#reperes-temps-legend` (HTML + CSS
  `.hud-reperes-legend`) a été retiré.
- Comme `formatTemps()` lit déjà `state.timeUnit`, changer l'unité via le
  sélecteur `#unit-toggle` existant change aussi l'affichage du cadre —
  aucun nouveau contrôle nécessaire. `dessinerCadreTempsEnglobant()` ne
  prend donc plus `cutoffN` en argument (les graduations dépendent
  uniquement de `ranges.t`, pas du nombre de points affichés).

## 27. Durcissement de `docker-compose.yml` pour la production (04/08/2026)

Analyse complète de `docker-compose.yml` demandée par l'utilisateur, suivie
d'une correction de tous les points relevés. Testé de bout en bout en local
(authentification MQTT, healthchecks, limites de ressources, port Kafka non
publié, bucket renommé) — un message publié avec les bons identifiants
traverse correctement mosquitto → bridge → Kafka → kafka-consumer → InfluxDB
(bucket `Capteurs`).

### Sécurité

- **Authentification MQTT obligatoire** : `mosquitto.conf` passe à
  `allow_anonymous false` + `password_file`. Nouveau script
  `generer_mosquitto_password.sh` (utilise l'image `eclipse-mosquitto`
  elle-même via `docker run`, aucune installation locale requise) génère
  `mosquitto_password.txt` (gitignored) depuis `MQTT_USERNAME`/`MQTT_PASSWORD`
  dans `.env`. **Piège rencontré et corrigé** : le fichier généré est monté
  **sans** `:ro` — un montage en lecture seule empêche l'entrypoint de
  l'image mosquitto de `chown` le fichier vers l'utilisateur non-root sous
  lequel le broker tourne réellement, provoquant "Unable to open pwfile" au
  démarrage (observé avec Docker Desktop/Windows, dont les montages bind
  NTFS n'exposent pas les vraies permissions POSIX — `chmod` côté hôte n'a
  aucun effet). mosquitto ne réécrit jamais ce fichier en fonctionnement
  normal, donc l'absence de `:ro` ne l'expose pas à une modification
  involontaire par le conteneur.
- **`MQTT_USERNAME`/`MQTT_PASSWORD`** ajoutés aux 4 scripts qui se
  connectent en MQTT (`ingestion_capteurs_bluetooth.py`, `ingestion_dewesoft.py`,
  `ingestion_dewesoft_dxd.py`, `bridge_mqtt_to_kafka.py`) — vides par défaut
  (pas d'authentification tentée) pour ne pas casser un usage contre un
  broker encore en `allow_anonymous` ; doivent être configurés dans
  l'environnement local du PC labo Windows et du Raspberry Pi (Amiens),
  séparément du `.env` du VPS.
- **TLS pour les clients distants** : nouveaux `mosquitto.prod.conf` (listener
  8883 avec certificat) + `docker-compose.prod.yml` (override, utilise la
  syntaxe `!override` de la Compose Specification pour remplacer entièrement
  `ports`/`volumes` du service mosquitto plutôt que les fusionner). Activé
  via `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`,
  nécessite des certificats dans `certs/` (non fournis — voir `certs/README.md`,
  typiquement Let's Encrypt/certbot). Le listener interne 1883 (non publié
  vers l'hôte dans ce mode) reste en clair pour le trafic conteneur-à-conteneur
  (bridge), qui ne quitte jamais la machine.
- **Secrets obligatoires, plus de valeur par défaut faible** : `INFLUX_TOKEN`,
  `INFLUX_PASSWORD` (nouveau — le mot de passe admin InfluxDB était codé en
  dur `password_frd_test`, jamais paramétrable), `GRAFANA_ADMIN_PASSWORD`,
  `MQTT_USERNAME`, `MQTT_PASSWORD` utilisent tous la syntaxe
  `${VAR:?message}` — `docker compose up` refuse de démarrer si `.env` est
  incomplet, plutôt que de retomber silencieusement sur une valeur devinable.
- **Port Kafka 9092 non publié vers l'hôte** : seuls `bridge` et
  `kafka-consumer` (même réseau docker-compose) en ont besoin, via
  `kafka:9092` — la publication vers l'hôte était de toute façon
  partiellement inopérante pour un vrai client externe
  (`KAFKA_CFG_ADVERTISED_LISTENERS` annonce le nom interne "kafka") et
  élargissait la surface d'attaque (listener en clair, sans SASL) sans
  bénéfice.

### Fiabilité — parité avec les manifests Kubernetes

- **`KAFKA_HEAP_OPTS=-Xmx512m -Xms512m`** ajouté au service `kafka` — même
  réglage que `k8s/kafka/statefulset.yaml`, qui documente ce réglage comme
  nécessaire pour éviter un dimensionnement de heap imprévisible pouvant
  provoquer un crash-loop "unable to register with controller quorum"
  (cf. section "Points ouverts", correctif Kubernetes du 03/08/2026).
- **Limites de ressources** (`mem_limit`/`cpus`, syntaxe "legacy" garantie de
  fonctionner hors Swarm, plutôt que `deploy.resources` historiquement
  réservé à Swarm) sur tous les services.
- **Healthchecks** ajoutés à `mosquitto`, `kafka`, `influxdb`, `grafana` +
  `depends_on: ... condition: service_healthy` sur `bridge`, `kafka-consumer`,
  `grafana`. Risque atténué mais réel avant ce correctif : `depends_on` sans
  condition attend seulement le démarrage du conteneur, pas sa disponibilité
  réelle — la résilience applicative (retry Kafka déjà en place côté
  `bridge_mqtt_to_kafka.py`/`kafka_consumer_influx.py`) compensait, mais
  sans garantie d'ordre propre.

### Autres

- **Bucket InfluxDB renommé** `Test_Capteurs` → `Capteurs` (paramétrable via
  `INFLUX_BUCKET`, défaut `Capteurs`) — l'ancien nom trahissait une origine
  de test restée dans la config par défaut. Mis à jour partout où il est
  référencé côté docker-compose : `docker-compose.yml`,
  `kafka_consumer_influx.py` (valeur par défaut du fallback),
  `grafana/provisioning/datasources/influxdb.yml` (`${INFLUX_BUCKET}`,
  substitution vérifiée empiriquement fonctionnelle dans ce fichier de
  provisioning). **Non répercuté côté Kubernetes** (`k8s/influxdb/statefulset.yaml`,
  `k8s/bridge-mqtt-kafka/configmap.yaml`) : ce sont deux déploiements
  indépendants, celui-ci n'est pas remis en cause après sa validation du
  03/08/2026.
- **Avertissement de non-coexistence** ajouté en tête de `docker-compose.yml` :
  ne pas faire tourner cette pile et le déploiement Kubernetes (k3s) en même
  temps sur le même VPS — mêmes noms de service/ports par défaut, collision
  vécue concrètement en local le 03/08/2026.
- `.env.example` mis à jour avec toutes les nouvelles variables et leur rôle.
- **Conséquence concrète confirmée le 12/08/2026** : la bascule vers k3s
  (section 28) ayant rendu Kubernetes la **seule** pile réellement vivante
  sur le VPS, le nom "Capteurs" du paragraphe ci-dessus n'a jamais existé
  ailleurs que dans docker-compose (jamais déployé en prod). Vérifié en
  direct (`influx bucket list` sur `influxdb-0`) : le bucket réel du VPS
  s'appelle toujours **`Test_Capteurs`**, "Capteurs" n'existe pas. Les
  valeurs par défaut `INFLUX_BUCKET` de `backfill_hr_t.py`,
  `backfill_teneur_eau.py`, `kafka_consumer_influx.py` et du nouveau
  `murmetric_webapp/backend` corrigées en conséquence (`Test_Capteurs`) —
  elles pointaient toutes vers un nom qui trahissait l'intention
  docker-compose plutôt que la réalité du bucket effectivement utilisé par
  tous les backfills déjà exécutés (HR/T, teneur en eau) et par le pipeline
  Kafka→InfluxDB live.

## 28. Bascule vers Kubernetes (k3s) avec TLS pour la scalabilité automatique (04/08/2026)

**Contexte / décision produit.** Le nombre de parois et de capteurs suivis va
augmenter fortement à court terme. `docker-compose.yml` (section 27) n'offre
aucune scalabilité automatique — c'est exactement ce que le déploiement
Kubernetes (`k8s/`) apporte via le `HorizontalPodAutoscaler` de
`kafka-consumer-influx` (1 à 6 replicas selon le CPU, cf. section 12/`k8s/kafka-consumer-influx/hpa.yaml`).
Décision : **basculer la production du VPS de docker-compose vers Kubernetes**,
pas les faire cohabiter (cf. avertissement de non-coexistence, section 27).
Le VPS n'a pas de nom de domaine → un certificat Let's Encrypt est impossible
(validation de domaine requise) → un **certificat TLS auto-signé** est utilisé
à la place, viable ici car les deux extrémités (broker VPS, clients PC
Amiens/RPi) sont contrôlées par la même personne et peuvent explicitement
faire confiance à ce certificat précis (pas de chaîne de confiance publique
nécessaire).

**k3s installé en mode `--docker`** (`curl -sfL https://get.k3s.io | sh -s -
--docker`) plutôt que le containerd embarqué par défaut — partage le moteur
Docker déjà présent sur le VPS, donc les images buildées localement
(`murmetric-bridge:latest`, `murmetric-kafka-consumer:latest`, déjà
construites pour docker-compose) restent visibles telles quelles, sans export/
import manuel. VPS constaté ARM64 (Oracle Ampere A1) — sans incidence, les
images utilisées (`bitnamilegacy/kafka:3.7`, `python:*-slim`, etc.) étaient
déjà validées ARM64 via docker-compose sur cette même machine.
`metrics-server` et Traefik sont inclus par défaut dans k3s (le job Helm
d'installation de Traefik a échoué une première fois puis réussi seul au
redémarrage automatique — comportement normal, pas une panne à corriger).

**Certificat auto-signé.** Généré directement sur le VPS (la clé privée ne
quitte jamais la machine) :
```
openssl req -x509 -nodes -newkey rsa:2048 -keyout privkey.pem -out fullchain.pem \
  -days 3650 -subj '/CN=<IP_VPS>/O=MurMetric-FRD-CODEM' \
  -addext 'subjectAltName=IP:<IP_VPS>'
```
Le SAN (`subjectAltName`) doit être l'**adresse IP publique**, pas un nom
DNS — les clients (paho-mqtt/`ssl`) valident le nom du serveur contre ce SAN
lors du handshake TLS ; une IP littérale dans `connect()` ne correspond qu'à
un SAN de type `IP:`. Importé comme Secret Kubernetes de type `tls` :
`kubectl create secret tls mosquitto-tls --cert=fullchain.pem --key=privkey.pem -n murmetric`
(non versionné, généré manuellement à chaque déploiement).

**Manifests Kubernetes mis à niveau pour la parité de sécurité avec
docker-compose (section 27) :**
- `k8s/mosquitto/configmap.yaml` : double listener — `1883` (interne au
  cluster uniquement, jamais exposé) et `8883` (TLS, exposé publiquement),
  tous deux avec `allow_anonymous false` / `password_file`. Nécessite
  `per_listener_settings true` en tête de fichier — **sans cette option,
  mosquitto refuse de démarrer** (`Error: Duplicate password_file value in
  configuration`) dès que `allow_anonymous`/`password_file`/`certfile` sont
  redéfinis par listener ; sans cette directive, mosquitto les traite comme
  des réglages globaux et rejette leur répétition. Bug rencontré et corrigé
  lors du premier déploiement (`mosquitto` en `CrashLoopBackOff`).
- `k8s/mosquitto/deployment.yaml` : le fichier de mots de passe (Secret,
  clé `mosquitto-password-file`) et les certificats (Secret `mosquitto-tls`)
  sont montés dans des répertoires **séparés** de la ConfigMap
  (`/mosquitto/secrets`, `/mosquitto/certs`) plutôt que dans
  `/mosquitto/config` — un `Pod` ne peut pas superposer deux sources de
  volume différentes (ConfigMap + Secret) au même chemin sans `subPath`.
  Les `chown` échoués au démarrage du conteneur (montages en lecture seule,
  visibles dans les logs : `Read-only file system`) sont **sans
  conséquence** : les fichiers montés depuis un Secret Kubernetes sont
  lisibles par tous par défaut (mode `0644`), donc l'utilisateur non-root
  `mosquitto` peut les lire même sans en devenir propriétaire — à ne pas
  confondre avec le bug de permissions docker-compose (section 27), dont la
  cause et la portée sont différentes.
- `k8s/mosquitto/service.yaml` : **scindé en deux `Service` distincts** — un
  seul objet `Service` ne peut pas avoir un port en `ClusterIP` et un autre
  en `LoadBalancer`. `mosquitto` (`ClusterIP`, port 1883) reste le point
  d'entrée interne utilisé par `bridge-mqtt-kafka` (aucun changement côté
  `murmetric-config`) ; `mosquitto-external` (`LoadBalancer`, port 8883)
  expose le TLS publiquement, via le `ServiceLB` intégré de k3s (règles
  `iptables` DNAT — pas de socket visible en `LISTEN` côté hôte, c'est
  normal, le trafic est redirigé au niveau noyau).
- `k8s/bridge-mqtt-kafka/deployment.yaml` : ajout de `MQTT_USERNAME`/
  `MQTT_PASSWORD` via `secretKeyRef` (`murmetric-secrets`) — absents
  jusqu'ici, le bridge se connectait donc en anonyme (silencieusement
  toléré tant que `k8s/mosquitto/configmap.yaml` avait `allow_anonymous
  true`).
- `k8s/secrets.yaml.template` étendu avec `mqtt-username`, `mqtt-password`,
  `mosquitto-password-file` (contenu du fichier de hash bcrypt généré par
  `generer_mosquitto_password.sh`, encodé en base64). Le vrai
  `k8s/secrets.yaml` a été généré **directement sur le VPS** à partir des
  valeurs déjà présentes dans `.env`/`mosquitto_password.txt` (mêmes
  identifiants MQTT que docker-compose, puisque les deux déploiements ne
  tournent jamais simultanément) — jamais affichées ni transmises en clair.

**Support TLS côté client** ajouté à `ingestion_dewesoft.py`,
`ingestion_dewesoft_dxd.py`, `ingestion_capteurs_bluetooth.py` (les 3
scripts qui tournent sur le PC labo Windows/RPi à Amiens) : nouvelles
variables d'environnement `MQTT_TLS_ENABLED` et `MQTT_CA_CERT`, appelant
`mqtt_client.tls_set(ca_certs=MQTT_CA_CERT)` avant `connect()` quand activé.
`bridge_mqtt_to_kafka.py` (tourne côté VPS, ne parle qu'en interne au
cluster) n'a **pas** reçu ce support — volontairement, le trafic
bridge↔mosquitto ne quitte jamais le réseau interne k8s.

**Test de bout en bout réalisé** (même rigueur que docker-compose, section
27) : publication authentifiée + TLS depuis un pod jetable dans le cluster
(`mosquitto_pub -h mosquitto-external -p 8883 --cafile ... -u ... -P ...`,
avec `--insecure` pour ignorer la vérification du nom d'hôte uniquement —
le certificat ne couvre que l'IP publique, or le test se fait via le nom de
Service interne ; un vrai client distant se connectant par l'IP publique
n'a pas ce problème), suivie d'une vérification directe dans InfluxDB
(plage de temps absolue, cf. piège de la section 12) confirmant l'écriture
de la valeur de test, puis nettoyage du pod et du point de test.
`kubectl top pods` / `kubectl get hpa` confirment que `metrics-server`
fonctionne réellement (CPU rapporté par pod, HPA actif à `5%/70%`).

**Point ouvert / action utilisateur requise :** l'accessibilité **externe**
réelle du port 8883 (depuis Internet, pas depuis l'intérieur du cluster) n'a
pas pu être vérifiée depuis l'environnement d'exécution de Claude Code — un
test de connexion TCP échoue de façon identique sur le port 8883 (nouveau)
et sur le port 1883 (déjà fonctionnel en docker-compose), alors qu'un test
de contrôle vers un hôte externe quelconque réussit instantanément ; tout
indique que l'adresse IP sortante de cet environnement n'est simplement pas
autorisée par le Security List Oracle Cloud du VPS, pas un problème réel du
port 8883. Le port 8883 doit être ajouté au Security List / Network
Security Group Oracle Cloud (VCN → Security Lists → règle ingress TCP 8883
depuis `0.0.0.0/0`) avant de considérer le nouveau chemin TLS opérationnel
pour de vrais clients distants — probablement déjà fait pour 1883/3000/8086
lors du déploiement docker-compose initial, mais 8883 est un port
supplémentaire, pas automatiquement couvert.

## 29. Backfill HR/T — import des exports historiques BlueMaestro (11/08/2026)

Objectif : faire entrer en base l'historique des 59 capteurs BlueMaestro déjà
embarqués dans les deux maquettes (`data_HR_T/T et HR/`, ~14 mois d'export,
un dossier par relevé terrain), avant que le Raspberry Pi ne prenne le relais
en continu (section 31). Contrairement au retrait (fichiers `.dxd` traités
au fil de l'eau), ce backfill est un import ponctuel — script rejouable mais
pas destiné à tourner en continu.

### Source des données et pièges trouvés

- **Format des exports** : deux variantes selon la date/l'exportateur — un
  format ancien sans en-tête (24/12/2025 uniquement), et un format récent
  avec en-tête (`Device MAC`, `Number of Logs`, `Start/End Date`...). Parmi
  les exports récents, **un lot entier** (43 fichiers du 08/01/2026, exporté
  par une personne différente — `achrafcharaka@gmail.com` vs
  `bourbiasofiane@batlab.fr` dans les autres lots) utilise **`;` comme
  séparateur et `,` comme décimale** (paramètres régionaux Excel FR) au lieu
  du format standard — un `csv.reader` réglé sur `,` ignore silencieusement
  tout le fichier (une seule "colonne" par ligne). Détection automatique du
  délimiteur ajoutée (`backfill_hr_t.py`), sur la ligne d'en-tête de données
  plutôt qu'un séparateur fixe.
- **Exports cumulatifs, pas incrémentaux** : chaque prélèvement contient déjà
  tout l'historique du logger depuis son `Start Date` d'origine — vérifié
  empiriquement sur plusieurs capteurs (le `Number of Logs` grandit, le
  `Start Date` ne bouge pas). Le backfill n'utilise donc que **le prélèvement
  le plus récent disponible par capteur**, jamais un cumul de plusieurs
  fichiers — le plus récent est un sur-ensemble strict des précédents pour ce
  capteur.
- **Historique antérieur à l'installation en paroi** : certains loggers
  contiennent des mesures dès mai 2025 (test/calibration avant pose), bien
  avant le début de campagne documenté (01/12/2025). Filtrées via
  `HR_T_DATE_DEBUT` (01/12/2025 par défaut) — seuil global approximatif, pas
  une date d'installation par position ; à affiner si une source plus
  précise existe.
- **Mapping capteur → mur/couche/position** : reconstruit depuis la feuille
  "Répartition ds les 2 maquettes" du classeur `Identification des capteurs
  bluemaestro.xlsx` (colonnes B/F, primaires — un numéro = une position, non
  ambigu), **étendu aux capteurs 60-75** (positions ajoutées ~30/03/2026,
  lignes 39-47 de la même feuille). Les colonnes A/E de cette feuille
  (candidats de remplacement suite à pile déchargée) sont **volontairement
  non résolues automatiquement** : plusieurs numéros y apparaissent comme
  candidat pour plusieurs positions différentes selon la ligne, sans date de
  bascule associée — un capteur de remplacement garde donc son mapping
  primaire, jamais celui d'une position qu'il aurait pu remplacer
  temporairement. Un futur repli sur ce mapping n'est envisageable qu'avec
  une source de dates de remplacement fiable (la personne ayant tenu ce
  classeur, ou reconstruction empirique par croisement des fenêtres de
  collecte CSV).
- **Nomenclature du mur** : le classeur "Répartition" nomme les deux
  maquettes "Maquette 1"/"Maquette 2" (par taille de granulat), sans lien
  direct visible avec les noms `SOCMA 1`/`SOCMA 2` du retrait
  (`capteurs_retrait.json`). Résolu par recoupement des canaux DeweSoft sur
  le schéma `Dessin des deux maquettes.pptx` : le bloc dessiné en premier
  ("SOCMA 2" sur ce schéma) porte les canaux `VA1/VA2/HA1/HA2`, qui sont
  formellement `SOCMA 1` dans `capteurs_retrait.json` — confirmation que
  "Maquette 1" = `SOCMA 1`, "Maquette 2" = `SOCMA 2` (validé par
  l'utilisateur). Retenu explicitement, pour permettre de croiser retrait et
  HR/T sur `nom_mur` sans traduction supplémentaire.

### Identité provisoire des capteurs

Les 59 capteurs de maquette n'ont **jamais été vus par un scan BLE réel** —
seuls les 4 premiers octets de leur MAC sont connus (hexID des exports
BlueMaestro, ex. `EFF1F80F`), confirmés par un test physique (étiquette
capteur + plateforme BlueMaestro) comme étant exactement les 4 premiers
octets de la vraie MAC 6 octets, jamais les 2 derniers. `capteurs.json`
accepte donc une **clé provisoire à 8 caractères hex, sans `:`** (regex
dédiée, distincte du format MAC réel — aucune confusion possible), marquée
`mac_complete_connue: false`. Chaque point InfluxDB backfillé porte en plus
un field `mac_complete_connue=false` pour retrouver facilement, une fois le
Raspberry Pi déployé (section 31), tout ce qui reste à réconcilier — les 4
premiers octets étant déjà connus, la correspondance sera immédiate.

**Conséquence assumée** : la clé provisoire devient le tag `adresse_mac` en
InfluxDB. Réconcilier avec la vraie MAC plus tard ne corrigera pas
automatiquement les points déjà écrits (les tags sont figés à l'écriture) —
une migration ciblée (suppression par `adresse_mac=<hexID>` + réécriture)
sera nécessaire à ce moment-là. Accepté : `nom_mur`/`nom_couche`/`position`
(les tags qui comptent pour l'usage réel — Grafana, croisement avec le
retrait) sont corrects dès l'écriture et ne dépendent pas de cette identité
technique.

### Schéma InfluxDB — `mesures_capteurs` enrichi

Décision alignée sur le retrait : `mesures_capteurs` porte désormais
`nom_mur`/`nom_couche`/`position`/`rd` **directement en tags**, comme
`mesures_dewesoft`, plutôt que de n'avoir que `adresse_mac`/`emplacement`/
`nom_capteur` et nécessiter une jointure sur `registre_capteurs`. Implique
de propager ces champs depuis `capteurs.json` jusqu'au payload MQTT publié
par `ingestion_capteurs_bluetooth.py` (pas seulement le backfill) —
`construire_point_capteurs()` (`kafka_consumer_influx.py`) mis à jour en
conséquence, pour que backfill et flux live du Raspberry Pi produisent des
points strictement compatibles.

### Exécution

Écriture **directe** en InfluxDB (line protocol, réutilisant la même
structure de tags que `construire_point_capteurs()`), **sans passer par
MQTT/Kafka** : ~45 000 points contre 1,5 milliard pour le retrait, la
chaîne de résilience MQTT→Kafka (pensée pour un flux continu distant et un
volume massif) n'apporte rien pour un import ponctuel depuis des fichiers
déjà sur disque. `backfill_hr_t.py` : dry-run par défaut (aperçu complet,
rien n'est écrit), `--confirmer` pour l'écriture réelle. Contournement
technique retenu pour l'écriture depuis un poste hors du cluster : le pod
InfluxDB n'a pas `socat` (`kubectl port-forward` impossible) — génération du
fichier line-protocol en local, transfert par `kubectl cp`, chargement via
`influx write` exécuté dans le pod.

**Résultat (11/08/2026)** : 45 132 lignes générées, 45 098 points uniques
écrits (34 doublons exacts mesure+tags+timestamp déjà présents dans les CSV
sources — comportement attendu d'InfluxDB, pas une perte). Fenêtre réelle
01/12/2025 → 10/07/2026. Vérifié par relecture directe de plusieurs points
(valeurs cohérentes avec l'aperçu pré-écriture) et par une lecture agrégée
(températures moyennes plausibles par mur/position/couche, y compris un
écart physique cohérent entre les deux maquettes à l'interface extérieure).

## 30. Support multi-marques BLE — ELA Innovation (11/08/2026)

Un second modèle de capteur température/humidité entre dans le parc, en plus
du Blue Maestro Disc Maxi : **ELA Innovation Blue Puck RHT** (réf.
IDF25242-CC, pile 3V, jusqu'à 18 ans d'autonomie annoncée, IP65). Protocole
entièrement différent — nécessite un second décodeur, pas une simple
variante du premier.

### Protocole (source : ELA Innovation, "BLE Frame specifications" v12B)

- **Company ID Bluetooth SIG : `0x0757`** (vs `0x0133` pour Blue Maestro).
- **Deux modes d'annonce possibles**, tous deux gérés sans configuration NFC
  préalable requise :
  - **"Service Data"** (mode usine par défaut, confirmé par test physique
    sur l'exemplaire réel) : deux blocs `service_data` distincts, sur les
    UUID caractéristiques Bluetooth SIG standard température (`0x2A6E`,
    int16 little-endian ×0,01°C signé) et humidité (`0x2A6F`, uint8 %).
  - **"Manufacturer Specific Data"** (à activer explicitement via l'outil
    NFC ELA) : après le company ID, un octet `RHT_DATA_ID` (`0x21`,
    identifie une trame RHT — les autres formats ELA, ID/T seul/MAG/MOV...,
    ont un octet différent et sont ignorés), l'humidité (1 octet %), un
    octet `TEMP_DATA_ID` (`0x12`), puis la température (int16 LE ×0,01°C
    signé).
- Validé contre l'exemple officiel de la documentation ELA (27,44°C, 48% HR)
  **et** contre le capteur physique réel (`ingestion_capteurs_bluetooth.py`
  détecte "P RHT 9078CF" en mode Service Data, valeurs stables et
  plausibles sur plusieurs lectures).

### Intégration dans `ingestion_capteurs_bluetooth.py`

Le callback BLE essaie les décodeurs dans l'ordre (Blue Maestro via
`manufacturer_data[0x0133]`, puis ELA via `manufacturer_data[0x0757]`, puis
ELA via `service_data`) ; le premier qui reconnaît le paquet gagne. Le reste
du pipeline (`capteurs.json`, MQTT, Kafka, InfluxDB) ne change pas — un
capteur ELA est traité comme n'importe quel autre une fois décodé.

**Nouveau champ `famille_capteur`** (`"bluemaestro"`/`"ela"`, déterminé
automatiquement à l'auto-enregistrement) : la tâche de reconfiguration GATT
périodique (section 11) ne concerne que Blue Maestro (`setlog~`/`*lint`,
protocole Nordic UART) — le Blue Puck RHT se configure par NFC, pas par
GATT. Sans ce marquage, un capteur ELA apparaîtrait indéfiniment "non
configuré" (`lint_configure` n'a pas de sens pour lui) et déclencherait un
scan + pause de l'ingestion à chaque cycle (6h par défaut) pour rien. Absent
sur une entrée existante = traité comme `"bluemaestro"` (rétrocompatible).

**Bug latent corrigé au passage** : `capteurs.json` portait un BOM UTF-8
(origine antérieure à cette session) — les 3 points de lecture du fichier
dans `ingestion_capteurs_bluetooth.py` ouvraient en `utf-8` strict, qui
plante sur un BOM (`JSONDecodeError`), invalidant silencieusement le
hot-reload jusqu'à la prochaine écriture par le script lui-même. Lecture
passée en `utf-8-sig` (tolère BOM et absence de BOM).

## 31. Déploiement Raspberry Pi 5 — `murmetric_pi5` (12/08/2026)

Premier déploiement réel d'un Raspberry Pi pour l'ingestion BLE, avant
installation définitive à Amiens. Setup effectué à distance (le Pi restant
sur HDMI/sans clavier physique) :

- **Flashage headless** : carte micro-SD (connectée en USB, pas de lecteur
  SD natif sur le poste de flashage) via Raspberry Pi Imager, OS
  Customisation pré-configurée (hostname `murmetric-pi5`, Wi-Fi, SSH par mot
  de passe activé) — le Pi démarre déjà joignable en SSH, sans jamais
  brancher clavier/écran dessus.
- **Tailscale installé**, rejoint le même tailnet que `pc-blaidoudi` (PC
  Amiens) et le VPS — accès à distance conservé indépendamment du réseau
  physique une fois le Pi déplacé à Amiens (IP Tailscale fixe,
  `100.101.220.39`).
- **Projet dans `/home/murmetric/murmetric_pi5/`** avec un `.venv` dédié
  (`bleak`, `paho-mqtt`) — les scripts eux-mêmes restent versionnés à la
  racine du dépôt (comme pour le PC Windows Amiens), ce dossier n'est qu'un
  emplacement de déploiement, pas une restructuration du dépôt.
- **Antenne BLE USB externe** (StarTech, portée mesurée nettement supérieure
  au Bluetooth intégré du Pi5 — RSSI -22 à -28 dBm vs signal plus faible en
  interne sur les mêmes capteurs) : détectée sous `hci1`, mais **RF-kill
  logiciel actif par défaut** (`rfkill unblock` + `hciconfig hci1 up`
  nécessaires). `BLE_ADAPTER` (nouvelle variable d'environnement,
  `ingestion_capteurs_bluetooth.py` et `configure_capteurs.py`) cible `hci1`
  en priorité, avec **repli automatique** sur l'adaptateur par défaut
  (`demarrer_scanner_avec_repli()`) si l'antenne externe devient
  indisponible — sans effet sur Windows où le paramètre est ignoré par le
  backend WinRT.
- **Identifiants MQTT réutilisés depuis le PC Amiens** (`lancer_ingestion_dewesoft.bat`,
  même broker, même compte `murmetric`) plutôt que recréés — `lancer_ingestion_capteurs.sh`
  (gitignored comme le `.bat`, un `.example` versionné à la place) exporte
  les mêmes variables avant de lancer `start.py`.
- **Point d'entrée** : `start.py` (déjà existant, section 10) plutôt qu'un
  appel direct à `ingestion_capteurs_bluetooth.py` — préserve la phase 1
  (configuration GATT initiale bloquante) déjà conçue pour ce script, non
  utilisée si on avait démarré l'ingestion directement.
- **Service systemd** (`murmetric-capteurs.service`) : `Restart=always`,
  `StartLimitIntervalSec=0` (pas de limite de redémarrages, contrairement au
  défaut systemd de 5 échecs/10s), activé au démarrage. Logs via
  `journalctl -u murmetric-capteurs` — pas de fichier de log manuel
  (contrairement à Windows, où Task Scheduler n'a pas d'équivalent à
  journald). Un seul service suffit : la reconfiguration GATT périodique
  (`configure_capteurs.py`) est déjà appelée en tâche de fond par
  `ingestion_capteurs_bluetooth.py` lui-même (section 11), pas besoin d'une
  unité séparée.
- **Validé de bout en bout** : capteur ELA physique détecté et auto-enregistré
  (`famille_capteur: "ela"` correctement appliqué), 2 capteurs Blue Maestro
  déjà actifs décodés et publiés vers MQTT cloud → InfluxDB en conditions
  réelles pendant les tests. Ces 2 capteurs de test (Ateliers Troyes/Amiens,
  sans rapport avec les maquettes HR/T) désactivés (`ingestion: false`) et
  leurs points de test supprimés d'InfluxDB une fois la validation terminée,
  pour ne pas polluer la base avec du bruit de test.

### Réseau à Amiens — Ethernet prioritaire + Wi-Fi de secours (12/08/2026)

Anticipé avant le déménagement physique du Pi (le poste devient injoignable
en manipulation directe une fois posé à Amiens — toute reconfiguration
réseau doit être faite **avant**, pendant que l'accès Tailscale/SSH est
encore garanti par le réseau actuel). Constat côté PC labo Amiens
(`pc-blaidoudi`) : réseau **filaire actif** (Ethernet, deux cartes Realtek),
`wlansvc` non démarré — pas de Wi-Fi actif sur ce poste. Le Wi-Fi du labo
existe néanmoins comme réseau connu, nom **"Batlab_Wifi"**.

Deux profils NetworkManager configurés sur le Pi (`nmcli`, persistés dans
`/etc/NetworkManager/system-connections/`, indépendants des profils générés
par netplan) :
- `netplan-eth0` (Ethernet), `autoconnect-priority` relevée à 10 —
  connexion automatique par DHCP dès qu'un câble est branché, aucune
  configuration nécessaire sur place.
- `Batlab_Wifi` (nouveau), `autoconnect-priority` 5 — bascule automatique si
  aucun câble n'est disponible à l'emplacement final près des parois.

Les deux coexistent sans conflit (interfaces physiques différentes,
`eth0`/`wlan0`) : à l'arrivée à Amiens, le Pi rejoint automatiquement l'un
ou l'autre selon ce qui est physiquement disponible, sans intervention sur
site. Tailscale/SSH reviennent seuls dès qu'une des deux interfaces obtient
une adresse.

## 32. Interface applicative unifiée & assistant IA (conception, 12/08/2026)

Jusqu'ici, les données (retrait, HR/T, teneur en eau) ne sont exploitables
que via Grafana, des requêtes Flux manuelles ou des scripts — aucune
application ne les rassemble pour l'utilisateur final. L'abaque 3D
(section 18) est le seul artefact visuel existant, mais reste un **POC
autonome** figé sur un jeu de données statique, jamais relié à InfluxDB en
direct (section 15). Chantier démarré maintenant, en parallèle du
déploiement du Pi à Amiens (section 31) — aucune dépendance entre les deux.

### Type d'application et stack retenue

**Application web**, pas desktop : cohérent avec une architecture déjà
pensée à distance de bout en bout (VPS central, Grafana web, PC/Pi Amiens
accessibles via Tailscale) — se déploie comme un service supplémentaire
dans le namespace k3s `murmetric`, à côté de Mosquitto/Kafka/InfluxDB/
Grafana, sans changer d'écosystème.

- **Backend : FastAPI (Python)**, cohérent avec le reste du pipeline
  (ingestion, `kafka_consumer_influx.py`, scripts de backfill — tous déjà en
  Python), réutilise directement `influxdb_client`. Expose l'API de
  requêtage pour le frontend, les endpoints de saisie/édition, et
  l'endpoint de l'assistant IA.
- **Frontend : portage du moteur de rendu de l'abaque POC, pas réécriture.**
  `abaque-3d-hygrothermique.html` (2831 lignes, section 18) contient une
  logique de rendu Canvas déjà validée et non triviale : axes gradués
  (section 23), lecture de valeur par projection (section 24), graphiques
  compagnons valeur/temps (section 25), cadre temporel englobant
  (section 26), filtre de Hampel (section 17), fond personnalisable
  (section 22). À extraire en composant réutilisable ; seule sa source de
  données change — remplacer le JSON statique embarqué (généré depuis
  l'Excel de simulation via `combiner_donnees_reelles*.py`) par des appels
  à l'API backend, qui interroge InfluxDB en direct. Framework d'assemblage
  proposé : React (embarque facilement le composant abaque, l'iframe
  Grafana, les formulaires de saisie et le panneau de chat IA dans une
  interface cohérente) — pas encore validé en détail (composants, state
  management).
- **À ne pas réutiliser** : les scripts d'extraction Excel
  (`combiner_donnees_reelles*.py`, `extraire_*_reel.py`) — outillage
  exclusivement POC pour simuler une base de données à partir d'un classeur,
  obsolète dès que l'appli requête InfluxDB directement ; le nommage interne
  "Mur 1/Mur 2" du POC (→ "SOCMA 1"/"SOCMA 2" en production, section 19).

### Modules

1. **Vue d'ensemble / abaque** — moteur du POC porté en lecture live sur
   InfluxDB (cf. ci-dessus).
2. **Grafana embarqué** (iframe, panels choisis par l'utilisateur) —
   exigence produit déjà notée plus bas (point ouvert) : nécessite
   `allow_embedding` côté Grafana + une auth d'iframe (accès anonyme lecture
   seule sur dashboards partagés, ou proxy d'authentification). Non résolu
   techniquement à ce stade, à traiter dans ce chantier.
3. **Saisie/édition teneur en eau** — déjà entièrement conçu (section 16) :
   formulaire mur/couche/valeur (2 décimales)/commentaire/date, écriture
   directe InfluxDB sans Kafka, correction ultérieure via le triplet exact
   (mur, couche, date_mesure).
4. **Gestion des capteurs** — vue (lecture seule en V1) de ce qui est
   aujourd'hui `capteurs.json`/`capteurs_retrait.json` (registre
   d'étiquetage, section 19). Écriture depuis l'appli repoussée : suppose
   une source de configuration persistante et concurrente-safe, qu'un
   fichier JSON édité à la main par SSH ne garantit pas (cf. point ouvert
   ci-dessous).
5. **Assistant IA** — détaillé ci-dessous.

### Assistant IA — architecture

Le backend FastAPI expose un endpoint de chat qui appelle l'API Anthropic
(Claude), avec **tool use** pour interroger InfluxDB à la demande
(measurement, tags, période) plutôt que tout précharger dans le prompt.

**Modèle local (Ollama sur le VPS) vs API publique (tranché, 12/08/2026) :
API publique retenue.** Le VPS héberge déjà Ollama pour un autre usage
(`qwen2.5:14b`, `llama3.1`) — options comparées avant de décider. Specs
vérifiées : 4 vCPU, 23 Go RAM, **aucun GPU** ; ~13 Go déjà utilisés par les
services existants (le VPS est déjà sous tension partagée, cf. commit
`0ecaf9f` de redimensionnement des pods murmetric). Rejeté pour cet usage
précis :
- Inférence CPU-only sur un modèle ~14B = lente (dizaines de secondes à
  quelques minutes par réponse), incompatible avec un usage interactif
  ("expliquer une courbe" doit répondre vite).
- L'architecture retenue s'appuie sur le modèle pour interroger InfluxDB à
  la demande (tool use/function calling) — un modèle local de cette taille
  est nettement moins fiable sur cet aspect qu'un modèle frontier.
- Le brouillon de rapport d'instrumentation est un texte à usage
  professionnel — la qualité rédactionnelle d'un 14B local reste en retrait.
- Le coût token de l'API publique reste maîtrisable précisément grâce au
  garde-fou ci-dessous (jamais de données brutes envoyées, uniquement des
  stats pré-agrégées) : volume par requête faible et prévisible.
Point resté à confirmer (pas bloquant, aucun signal contraire identifié) :
absence de contrainte de confidentialité côté FRD-CODEM sur l'envoi de
données de mesure agrégées (pas de données personnelles) à l'API Anthropic.

Deux garde-fous jugés indispensables, pas optionnels :
- **Jamais de points bruts envoyés au modèle.** Un capteur peut porter des
  dizaines de milliers de points sur une campagne — le backend doit
  pré-agréger (stats descriptives, tendance, détection d'anomalie simple,
  via `aggregateWindow` Flux et/ou pandas côté Python) avant transmission.
  Sans ce pré-traitement, coût de contexte et fiabilité des réponses ne
  tiennent pas.
- **Ancrage explicite sur la sélection affichée.** Le frontend doit
  transmettre l'état courant (mur/couche/mesure/période affichée dans
  l'abaque ou le panel Grafana actif) avec chaque prompt — pas de
  déduction implicite côté serveur à partir du seul texte de la question.

**Deux modes retenus pour la V1** (décidé le 12/08/2026, les deux ensemble
plutôt qu'un seul en premier — même socle de pré-agrégation sert aux deux) :
- **(a) Explication de la courbe/sélection affichée** — texte généré à
  partir des statistiques pré-calculées + connaissances générales du
  modèle.
- **(b) Brouillon de rapport d'instrumentation** — génère un texte structuré
  (rapport de campagne) à partir des données agrégées d'une période/d'un ou
  plusieurs murs. Toujours un **brouillon à relire et corriger par
  l'utilisateur avant usage réel**, jamais un rapport final émis sans
  relecture humaine.

### Non tranché à ce stade

- ~~Mécanisme d'authentification exact~~ **Tranché et implémenté le
  12/08/2026 : JWT interne, comptes dans `users.json`** (cf. plus bas) —
  pas de SSO, échelle de l'outil ne le justifie pas pour l'instant.
- Où stocker durablement la configuration des capteurs si le module 4
  passe en écriture (rester sur fichiers JSON vs migrer vers une vraie
  base) — un fichier édité à la main par SSH ne se prête pas à des écritures
  concurrentes depuis une appli multi-utilisateurs.
- Détail technique de l'embarquement Grafana (`allow_embedding`, stratégie
  d'auth de l'iframe) — **et prérequis désormais plus fondamental** :
  Grafana n'est même pas joignable depuis Internet actuellement (cf.
  "Bloquant restant pour un accès public" ci-dessous), donc rien à
  embarquer tant que ce point réseau n'est pas résolu.
- Ouverture d'un port côté Security List Oracle Cloud pour l'accès public
  (cf. ci-dessous) — action utilisateur, hors de portée SSH/kubectl.

### Squelette de code (démarré, 12/08/2026)

Première brique concrète, dans `murmetric_webapp/` (nouveau dossier à la
racine du dépôt, même logique que `murmetric_pi5/` : un emplacement de
déploiement/développement, pas une restructuration du reste du dépôt) :

- **`murmetric_webapp/backend/`** (FastAPI) : `app/main.py` +
  `app/routers/{mesures,teneur_eau,capteurs,assistant}.py`. Endpoints
  fonctionnels et testés en local (health check, lecture `capteurs.json`/
  `capteurs_retrait.json`, requêtage InfluxDB générique par
  type/mur/couche/période, écriture/correction `mesures_teneur_eau` suivant
  exactement la sémantique field-vs-tag de la section ci-dessus, chat IA
  avec boucle tool-use Anthropic bornée à 4 itérations). Testé sans
  connexion InfluxDB réelle disponible en local (erreurs proprement
  renvoyées en 502, pas de 500 brut) — connexion réelle au bucket `Capteurs`
  du VPS à valider à la prochaine session avec les vraies variables
  d'environnement (`.env.example` fourni).
- **`murmetric_webapp/frontend/`** (React + Vite, `react-router-dom`) :
  4 pages (Vue d'ensemble, Teneur en eau, Capteurs, Assistant IA), build de
  production et serveur de dev tous deux vérifiés, CORS backend↔frontend
  validé. `SelecteurMesure` (composant partagé Vue d'ensemble/Assistant) et
  `GraphiqueSVG` (tracé minimal en SVG pur, **provisoire** — remplace
  temporairement le moteur Canvas de l'abaque POC, dont le portage réel
  reste à faire, cf. ci-dessus). Page Capteurs en lecture seule (conforme à
  la conception). Page Teneur en eau : saisie **et** correction déjà
  câblées (édition inline par ligne, transmet le triplet original exact
  au `PUT`, cf. conception section 16).

**Non fait à ce stade** (à ne pas déduire comme implémenté) : portage réel
du moteur Canvas de l'abaque, iframe Grafana, authentification (l'API
écrit actuellement avec un utilisateur provisoire en dur,
`UTILISATEUR_ID_PROVISOIRE`, **jamais** à utiliser tel quel en production).

### Déployé sur le VPS, connecté aux vraies données (12/08/2026)

`murmetric-webapp` tourne maintenant dans le namespace k3s `murmetric`,
même pattern que `kafka-consumer-influx` (`k8s/webapp/deployment.yaml` +
`service.yaml`, image `murmetric-webapp:latest` buildée localement sur le
nœud via `Dockerfile.webapp` — un seul conteneur, FastAPI sert à la fois
l'API et le build React statique, cf. `app/main.py`). Code déposé à plat
dans `/home/ubuntu/Projets_en_Production/murmetric/murmetric_webapp/` (même
convention que le reste : pas de clone git sur le VPS, fichiers transférés
directement).

**Connexion InfluxDB réelle validée en direct** (toutes les routes
testées avec de vraies requêtes contre le bucket `Test_Capteurs`, pas de
simulation) — trois bugs trouvés et corrigés au passage :
- `_valider_bornes()` : le calcul de repli "~1 an en arrière" était
  invalide (`fin_dt.replace(year=...)` ne soustrayait pas réellement un
  an dans la majorité des cas) → `cannot query an empty range` côté
  InfluxDB. Remplacé par un simple `timedelta(days=...)`.
- `/api/mesures/valeurs-tags` : liste Python interpolée directement dans
  la requête Flux (`group(columns: {tags})`) → syntaxe invalide (Flux
  attend des guillemets doubles, pas le `repr()` Python). Corrigé par un
  formatage explicite.
- **Statistiques sur `mesures_dewesoft` (retrait) trop lentes** : ce
  measurement pèse ~1,5 milliard de points (100 Hz). Un `aggregateWindow`
  ou un `min()`/`max()`/`mean()`/`count()` séquentiel sur un an dépassait
  systématiquement 30s, y compris en tentant un `reduce()` à passage
  unique (plus lent qu'attendu : pas d'optimisation de stockage comme les
  agrégats natifs). Deux correctifs cumulés : (1) les 4 agrégats natifs
  sont maintenant lancés **en parallèle** (`ThreadPoolExecutor`, le temps
  total tombe au niveau du plus lent des 4 plutôt que leur somme) ; (2) la
  fenêtre par défaut sans `debut`/`fin` explicite passe de 365 à **30
  jours pour "retrait" seulement** (`_FENETRE_DEFAUT_JOURS`) — hr_t et
  teneur en eau restent à 365 jours, volumes négligeables. `/api/mesures/
  valeurs-tags?type=retrait` ne requête même plus InfluxDB : il lit
  directement `capteurs_retrait.json` (source de vérité déjà existante
  pour le mapping canal→mur/couche/position, donc redondant d'interroger
  la base pour la même info, et bien plus rapide).
- Timeout du client InfluxDB relevé 10s → 30s (`app/influx.py`) pour
  laisser la marge nécessaire aux agrégations sur mesures_dewesoft.

**Découverte en creusant les vraies valeurs de tags** (`nom_couche` sur
`mesures_capteurs`) : ce ne sont **pas** les noms canoniques snake_case
documentés en section 16 (`carreau_ext`, `milieu_isolant`…) mais des
phrases libres issues telles quelles du `capteurs.json` HR/T (ex.
`"interface carreau et exterieur"`, `"interface isolant panneau
contreventement"`), avec en plus une incohérence de casse déjà présente
dans les vraies données (`"Milieu carreau"` et `"milieu carreau"`
coexistent). Seule la teneur en eau (backfillée par un script qui applique
un mapping explicite) utilise bien les noms canoniques. Le nouvel endpoint
`/api/mesures/valeurs-tags` existe précisément pour ça : peupler des menus
déroulants avec les valeurs qui existent vraiment plutôt que deviner un
nom "propre" côté frontend.

**Bloquant restant pour un accès public** (pas résolu, hors de portée
SSH/kubectl) : seuls les ports **22** et **8883** sont ouverts au niveau
de la **Security List Oracle Cloud** (pare-feu géré depuis la console
Oracle Cloud, en dehors de la VM elle-même). Vérifié empiriquement le
12/08/2026 : `curl` externe vers les ports 80, 443 et même 3000
(Grafana, dont le `Service` k8s est pourtant de type `LoadBalancer` et
fonctionne correctement en interne) **time out** — signature d'un
paquet silencieusement abandonné en amont de la VM, pas d'un service
absent (le port 8883/MQTT, lui, répond immédiatement). **Grafana n'a
donc jamais été réellement accessible depuis Internet**, indépendamment
du sujet "iframe/`allow_embedding`" déjà noté plus haut — un blocage plus
fondamental et jusqu'ici non identifié. Aucun accès à la console/API
Oracle Cloud disponible depuis cet environnement pour ouvrir un port
soi-même (pas de CLI `oci` configuré sur le VPS, pas de clé API dans
`F:\VPS_ORACLE_Ubuntu\`, seule la clé SSH y est stockée) — action à faire
par l'utilisateur dans la console Oracle Cloud. `k8s/webapp/service.yaml`
(port `8090`, LoadBalancer) est déjà prêt et fonctionnel dès que ce port
sera ouvert côté Oracle Cloud, aucun changement k8s à refaire à ce
moment-là.

**Résolu le 12/08/2026** : l'utilisateur a ouvert le port 8090 dans la
Security List Oracle Cloud. Vérifié depuis l'extérieur (pas seulement
depuis le VPS) : `http://89.168.34.201:8090` sert l'application avec une
vraie connexion InfluxDB (health check, frontend, requête statistiques
réelle sur `mesures_teneur_eau` — tout confirmé accessible publiquement).

### Grafana — dashboard, embedding, accès anonyme (12/08/2026)

Suite logique de "Grafana embarqué" (exigence produit notée plus haut) :
décision utilisateur explicite d'activer l'accès anonyme lecture seule
(nécessaire pour afficher l'iframe sans écran de connexion Grafana à
chaque visiteur — rôle Viewer uniquement, pas d'accès admin ni aux
identifiants de datasource) et l'embedding, plutôt que de passer par un
proxy same-origin via le backend (choix retenu pour l'instant : plus
rapide à mettre en place, même geste que l'ouverture du port 8090 ; le
proxy reste une amélioration possible plus tard).

- **Constat avant travail** : aucun dashboard n'existait dans Grafana
  (`GET /api/search` → `[]`) — l'exigence "Grafana embarqué" supposait
  jusqu'ici qu'il y avait quelque chose à embarquer, ce qui n'était pas
  le cas.
- **`k8s/grafana/deployment.yaml`** : ajout de `GF_SECURITY_ALLOW_EMBEDDING=true`,
  `GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer`.
  Vérifié : la réponse HTTP de `/d/<uid>` ne porte plus `X-Frame-Options`
  (absent, alors que Grafana l'envoie à `deny` par défaut) et répond 200
  sans authentification.
- **Premier dashboard provisionné** (pas créé à la main dans l'UI — même
  logique que la datasource, versionné et reproductible) :
  `k8s/grafana/dashboards/hr-t-socma.json` (température + humidité,
  couche "milieu isolant", groupé par `nom_mur` — cf. découverte sur les
  vraies valeurs de tags ci-dessus) + `k8s/grafana/dashboards-configmap.yaml`
  (deux ConfigMaps : `grafana-dashboard-provider` pour le fichier
  provider Grafana, `grafana-dashboards` pour le JSON lui-même — montés à
  des chemins différents, cf. commentaires dans le fichier). UID fixé en
  dur (`murmetric-hrt-socma`) pour une URL stable. Plage temporelle par
  défaut du dashboard : `now-1y` (pas `now-6h`) — la donnée HR/T est un
  backfill historique, pas un flux live (le Pi n'est pas encore à
  Amiens), une fenêtre récente serait vide par défaut.
- **Iframe câblée côté frontend** (`murmetric_webapp/frontend/src/pages/Grafana.jsx`,
  nouvel onglet "Grafana" dans la nav) — URL en dur vers l'IP publique du
  VPS (`http://89.168.34.201:3000/d/murmetric-hrt-socma?kiosk&theme=dark`),
  pas encore vers un chemin same-origin proxié.
- **Point bloquant identique à la webapp, pas encore résolu** : le port
  3000 (Grafana) doit être ouvert dans la Security List Oracle Cloud avant
  que l'iframe ne fonctionne réellement pour un visiteur externe — testé
  et validé uniquement en interne (depuis le VPS) à ce stade.

### Groq (LLM), authentification, paramètres modifiables, nomogramme (12/08/2026)

Suite de la même session — quatre chantiers tranchés/implémentés d'affilée,
tous déployés et validés en conditions réelles sur le VPS.

**Fournisseur LLM : Groq plutôt qu'Anthropic.** L'utilisateur a fourni des
identifiants Groq (app "MurMetric_AI" sur sa console) — remplace
l'intégration Anthropic initialement écrite. `murmetric_webapp/backend/app/routers/assistant.py`
réécrit pour l'API Groq (OpenAI-compatible, `https://api.groq.com/openai/v1`,
SDK `openai` plutôt que `anthropic`) : format de tool use différent
(`tools`/`tool_calls` façon OpenAI au lieu des content blocks Anthropic),
même boucle bornée à 4 itérations, mêmes garde-fous (jamais de points
bruts, uniquement des stats pré-agrégées). Modèle par défaut :
`llama-3.3-70b-versatile` (configurable). Ne change rien à la justification
"API cloud plutôt que LLM local sur le VPS partagé" (section précédente) —
Groq est aussi une API cloud externe, pas un modèle tournant sur le VPS ;
seul le fournisseur change. Comparaison factuelle demandée par l'utilisateur
entre les deux modèles Ollama déjà présents sur le VPS (pour une question
distincte, "lequel serait le plus adapté si on restait en local") :
`qwen2.5:14b` (14,8 milliards de paramètres, contexte 32k) jugé supérieur à
`llama3.1:latest` (8,0 milliards, contexte 128k) pour cet usage — sans
remettre en cause le choix Groq/cloud.
Validé en direct : appel réel à `/api/assistant/chat` (mode "explain",
sélection teneur en eau) → le modèle a appelé l'outil
`interroger_statistiques_mesures`, obtenu les vraies stats InfluxDB
(min 3,45 %, max 17,02 %, moyenne 8,07 %, 11 points), répondu correctement
en français à partir de ces chiffres.

**Authentification JWT implémentée** (`murmetric_webapp/backend/app/auth.py`
+ `routers/auth.py`) — comptes dans `users.json` (bcrypt), pas d'auto-
inscription ouverte : le tout premier compte est créé au démarrage via
`ADMIN_BOOTSTRAP_USERNAME`/`ADMIN_BOOTSTRAP_PASSWORD` (même logique que
`GF_SECURITY_ADMIN_PASSWORD` pour Grafana), les comptes suivants par un
utilisateur déjà connecté (`POST /api/auth/register`). `users.json` vit sur
un nouveau volume persistant dédié (`k8s/webapp/pvc.yaml`,
`murmetric-webapp-data`, monté sur `/data`) — **contrairement à**
`capteurs.json`/`capteurs_retrait.json`, copiés dans l'image Docker à
chaque build : les comptes doivent survivre à un rebuild, pas les données
capteurs (source de vérité = dépôt). `POST /api/teneur_eau` et
`PUT /api/teneur_eau` protégés (`Depends(utilisateur_courant)`), utilisent
désormais le vrai `username`/`nom_affiche` du compte connecté comme
`utilisateur_id`/`utilisateur_nom` — les constantes provisoires
(`UTILISATEUR_ID_PROVISOIRE`) supprimées de `config.py`.
**Compte modifiable depuis l'interface** (`PUT /api/auth/me`, page
"Paramètres" → "Mon compte") : changer nom d'utilisateur, mot de passe,
nom affiché, en confirmant le mot de passe actuel — répond à la question
explicite de l'utilisateur sur `admin`/`admin` (identifiants de bootstrap,
volontairement faibles, mais changeables en quelques clics).
Bootstrap réel : `admin`/`admin`, testé (`POST /api/auth/login` → JWT
valide → `GET /api/auth/me` → OK ; sans jeton → 401 sur une route
protégée).

**Clé API Groq modifiable depuis l'interface** (demande explicite de
l'utilisateur — jusque là uniquement figée en variable d'environnement au
déploiement) : nouveau module `app/parametres.py`
(`parametres.json`, même volume persistant que `users.json`), route
`GET`/`PUT /api/parametres` (authentifiée), page "Paramètres" → "Assistant
IA (Groq)". La clé n'est jamais réaffichée en clair après saisie (seuls les
4 derniers caractères, `obtenir_cle_groq()`/`masquer()`) — `GROQ_API_KEY`
(variable d'environnement, k8s secret `groq-api-key`) ne sert plus que de
valeur de repli initiale si rien n'a encore été défini depuis l'interface.

**Nomogramme — portage scopé (pas la parité complète du POC).** Après
inspection réelle du fichier POC (`abaque-3d-hygrothermique.html`, ~50
fonctions, rendu 3D par projection perspective custom écrit à la main —
`project()`/`render()` à eux seuls représentent plusieurs centaines de
lignes fortement couplées au DOM de la démo), un portage fidèle intégral
n'a pas été tenté dans cette session : risque de résultat bâclé plutôt
qu'un portage complet et fiable. Le périmètre porté correspond à la
comparaison POC/Grafana ci-dessus — uniquement ce que Grafana ne fait
structurellement pas :
- **Nouvel endpoint `GET /api/mesures/croisement`** (`mesures.py`) —
  apparie deux champs de la **même mesure** au même horodatage (ex.
  température vs humidité), via `aggregateWindow` + `pivot()` Flux.
  Volontairement limité à hr_t/retrait (deux champs d'une même mesure) :
  croiser avec la teneur en eau demanderait une jointure "au plus proche
  dans le temps" (mesure distincte, éparse, cf. section 16) — non traité
  dans ce premier périmètre.
- **`murmetric_webapp/frontend/src/components/Nomogramme.jsx`** — canvas
  2D pur (pas de dépendance de charting), sélection des deux grandeurs à
  croiser, points colorés selon leur position temporelle (bleu→rouge), et
  **lecture de valeur par projection au survol** (lignes pointillées vers
  les deux axes + étiquette de valeur) — reprise fidèle de l'intention de
  la section 24 du POC, dans le nouveau périmètre 2D.
  Affiché dans l'onglet "Vue d'ensemble", sous la courbe valeur/temps
  existante, pour les types hr_t/retrait uniquement.
- **`SelecteurMesure.jsx` amélioré au passage** : mur/couche passent de
  champs texte libres à des `<input list>` (autocomplete) peuplés par
  `/api/mesures/valeurs-tags` — corrige un défaut resté faux jusqu'ici
  (placeholder "carreau_ext", qui n'existe pas réellement en base, cf.
  découverte de la section précédente) sans empêcher de saisir une valeur
  pas encore vue.
Validé en direct : 3658 points température/humidité réels renvoyés pour
SOCMA 1 / "milieu isolant".

**Non fait dans ce premier périmètre** : graphiques compagnons (couverts
par Grafana), croisement avec la teneur en eau (jointure au plus proche à
implémenter séparément si besoin).

### Nomogramme 3D (13/08/2026)

Rotation 3D implémentée le lendemain comme prévu (report explicite de
l'utilisateur le 12/08, cf. entrée précédente — pas fait à la va-vite en
fin de session). Réécrit proprement plutôt que porté ligne à ligne depuis
le POC (dont le rendu 3D reste ~50 fonctions très couplées à son propre
DOM, jugé trop risqué à reprendre tel quel) — seule la formule de
projection en perspective (rotation yaw/pitch puis division perspective)
est reprise, c'est une technique 3D générique, pas du code spécifique au
POC.

- **Backend** : `GET /api/mesures/croisement` accepte désormais un
  `champ_z` optionnel (rétrocompatible — absent, comportement 2D inchangé).
  Avec 3 champs, le `pivot()` Flux renvoie des triplets (x, y, z) au même
  horodatage plutôt que des paires.
- **Frontend** : `murmetric_webapp/frontend/src/components/Nomogramme3D.jsx`
  — cadre en fil de fer (cube, repère spatial), points colorés selon leur
  position temporelle (même logique que la version 2D), triés par
  profondeur avant tracé (algorithme du peintre, pour une occlusion
  correcte), rotation à la souris (glisser = yaw/pitch), zoom à la molette,
  survol = lecture des 3 valeurs exactes du point le plus proche à l'écran.
  Bouton "Réinitialiser la vue".
- **Intégration** : bascule 2D/3D dans l'onglet "Vue d'ensemble", visible
  uniquement pour le type HR/T (3 grandeurs disponibles — température,
  humidité, point de rosée — condition nécessaire pour un vrai axe Z ;
  le retrait n'a que 2 champs, valeur brute/filtrée, pas de 3D proposée).
- Validé avec de vraies données : 3658 triplets température/humidité/point
  de rosée récupérés pour SOCMA 1 / milieu isolant.

### Composition libre des axes (HR/T × retrait) + options de vue (13/08/2026)

Suite immédiate — l'utilisateur a demandé pourquoi les axes ne pouvaient
pas mélanger HR/T et retrait (le nomogramme 3D initial limitait les 3 axes
à une seule mesure, comme la version 2D), et des options de vue façon POC.

- **Nouvel endpoint `GET /api/mesures/croisement-libre`** — contrairement à
  `/croisement` (une seule mesure, `pivot()` direct), interroge chaque axe
  **séparément** (HR/T et retrait ont des fréquences trop différentes pour
  un pivot commun — HR/T toutes les quelques heures, retrait 100 Hz),
  chacun agrégé sur la **même fenêtre** (`aggregateWindow`), puis aligne
  les séries en Python sur les horodatages communs (intersection des clés).
  Même leçon que retrait/mémoire réappliquée ici : dès qu'un axe retrait
  est impliqué, la fenêtre par défaut est plafonnée à 90 jours et
  s'applique à **tous** les axes de la requête (comparer des grandeurs sur
  des périodes différentes n'aurait pas de sens de toute façon). Chaque
  axe est décrit par une chaîne `"mesure:champ"` ou `"mesure:champ:canal"`
  (ex. `"retrait:valeur_filtree:HA1"`).
- **Frontend** (`Nomogramme3D.jsx`) : catalogue d'axes unifié (température,
  humidité, point de rosée, retrait filtré, retrait brut), sélecteur de
  canal qui apparaît dès qu'un axe retrait est choisi. Options de vue
  ajoutées : boutons de vues préréglées (Face/Dessus/Profil/Isométrique),
  rotation automatique (`requestAnimationFrame`, désactivée dès que
  l'utilisateur prend la main à la souris), légende de couleur (dégradé
  ancien→récent). Le nomogramme 3D n'est plus conditionné à `type === "hr_t"`
  dans `VueEnsemble.jsx` — accessible dès que HR/T ou retrait est
  sélectionné, puisqu'il peut désormais combiner les deux.
- **Point de vigilance découvert en testant** : sans dates explicites, la
  fenêtre par défaut (30 jours glissants côté retrait) peut renvoyer 0
  points — les données HR/T sont un backfill historique arrêté vers mai
  2026 (Pi pas encore déployé), le retrait continue en direct ; les deux
  périodes ne se chevauchent donc pas sur les 30 derniers jours. Validé
  avec une période explicite (avril 2026) : 407 points réels combinant
  température, humidité et retrait filtré (canal HA1) aux mêmes
  horodatages. Redeviendra pertinent par défaut une fois le HR/T live.

### Axe "Temps" dans le nomogramme 3D (13/08/2026)

Demande explicite : pouvoir mettre le temps sur un des 3 axes, avec un
choix d'unité (heures/jours/semaines/mois/années).

- **Backend** : `axe_y` de `/api/mesures/croisement-libre` devient
  optionnel (`axe_x` reste seul obligatoire) — nécessaire pour le cas où 2
  des 3 axes visuels sont "temps" (une seule grandeur réelle à
  interroger). Le temps lui-même n'est **pas** calculé côté serveur : ça
  reste une donnée dérivée du seul horodatage déjà renvoyé avec chaque
  point, pas la peine d'ajouter un concept d'axe temporel côté API.
- **Frontend** (`Nomogramme3D.jsx`) : "Temps" ajouté au catalogue d'axes.
  Le composant sépare les axes "réels" (envoyés au backend, dans l'ordre,
  sur `axe_x`/`axe_y`/`axe_z`) des axes "temps" (calculés localement :
  écart entre l'horodatage du point et l'horodatage le plus ancien du jeu
  de résultats, divisé par la durée de l'unité choisie), puis recompose
  chaque point final selon l'assignation visuelle voulue par
  l'utilisateur — ex. Axe X = Temps, Axe Y = Température, Axe Z = Retrait
  envoie seulement `axe_x=hr_t:temperature` au backend (un seul axe réel)
  et calcule X localement à partir de `point.time`. Sélecteur d'unité
  affiché dès qu'au moins un axe est "Temps".
- Validé : `croisement-libre` avec un seul axe réel (`axe_y`/`axe_z`
  absents) renvoie bien des points `{time, x}` sans erreur (56 points
  température testés sur une semaine réelle).

### Catalogue d'axes unifié 2D/3D (13/08/2026)

Demande explicite : le nomogramme 2D doit proposer exactement le même
catalogue d'axes que le 3D (grandeurs HR/T + retrait mélangeables, axe
Temps avec unité configurable), plutôt que sa propre liste limitée à une
seule mesure.

- **`murmetric_webapp/frontend/src/nomogrammeAxes.js`** (nouveau) —
  catalogue d'axes, unités de temps et fonctions utilitaires
  (`construireParamAxe`, `libelleGrandeur`) extraits en module partagé,
  utilisé par `Nomogramme.jsx` (2D) **et** `Nomogramme3D.jsx` — évite la
  duplication qui existait déjà à l'identique dans les deux fichiers.
- **`Nomogramme.jsx` (2D) réécrit** pour utiliser `/api/mesures/croisement-libre`
  (au lieu de l'ancien `/croisement`, resté disponible côté backend mais
  plus appelé par le frontend) — même logique de séparation axes
  réels/axe Temps que la version 3D, juste avec 2 rôles (`x`, `y`) au lieu
  de 3. `VueEnsemble.jsx` simplifié en conséquence : les deux composants
  ne prennent plus que `mur`/`couche` en props (le `type`/`position` de la
  sélection HR/T-vs-retrait n'a plus de sens une fois les axes composables
  librement entre les deux mesures).

### Type de tracé (nuage/trait) — 2D et 3D (13/08/2026)

Demande explicite, façon POC : choisir entre nuage de points (comportement
d'origine) et trait fin reliant les points dans l'ordre chronologique
(trajectoire dans l'espace des grandeurs), ou les deux ensemble.

- `TYPES_TRACE` ajouté à `nomogrammeAxes.js` (`nuage` / `trait` /
  `nuage_trait`), sélecteur dans les deux composants.
- **2D** : le trait relie les points consécutifs dans l'ordre déjà renvoyé
  par le backend (trié par `_time`), chaque segment coloré selon la
  position temporelle de son point de départ (même dégradé que les points).
- **3D** : point d'attention — le trait suit l'ordre **chronologique**
  (ordre d'arrivée), alors que les points restent triés par **profondeur**
  pour une occlusion correcte (algorithme du peintre) : ce sont deux
  ordres différents, calculés séparément à partir des mêmes coordonnées
  projetées. En mode "trait" seul (sans nuage), un marqueur reste affiché
  au survol pour ne pas perdre la lecture de valeur.

### Correspondances par projection (x→y, y→x) — 2D et 3D (13/08/2026)

Distinct du survol existant (qui lit un point déjà présent) : ici
l'utilisateur choisit une **valeur cible** sur un axe, et l'outil trouve
où la trajectoire (points dans l'ordre chronologique, traités comme des
segments reliés même en mode "nuage") **croise** cette valeur, en
interpolant linéairement les autres axes à cet endroit précis — comme la
lecture par projection du POC (section 24), en plus général (valeur libre,
pas seulement un point déjà mesuré).

- `trouverCroisements(points, axeCible, valeurCible, autresAxes)` ajouté à
  `nomogrammeAxes.js` — parcourt les segments consécutifs, teste si la
  valeur cible tombe entre les deux extrémités, interpole si oui. Une
  trajectoire qui va-et-vient peut croiser une valeur plusieurs fois :
  tous les croisements sont renvoyés, pas seulement le premier.
- **2D** : deux champs ("Trouver Y pour X =" / "Trouver X pour Y ="),
  résultats affichés en texte (vert pour x→y, orange pour y→x) et projetés
  sur le graphique (lignes pointillées vers les axes, coexistent avec le
  survol).
- **3D** : un sélecteur "Axe de référence" (X/Y/Z) + une valeur cible —
  les deux **autres** axes sont lus à chaque croisement. Marqueurs verts
  (cercle + halo) sur la trajectoire aux points de croisement.

### Date d'expiration de clé API trackée (12/08/2026)

Demande explicite : les identifiants API du LLM doivent être modifiables
depuis l'interface, avec leur date d'expiration (11/08/2027 pour la clé
Groq fournie). Champ `groq_api_key_expiration` ajouté à `app/parametres.py`
(même stockage que la clé/le modèle) et à la page "Paramètres" — saisie
manuelle (ni Groq ni InfluxDB n'exposent cette info via API), avertissement
visuel sous 30 jours avant échéance.

### Logo MurMetric (12/08/2026)

Mark en zigzag façon signal capteur (la pointe = une mesure) + wordmark
"MurMetric" + tagline "by FRD-CODEM", cohérent avec le thème sombre de
l'appli — `murmetric_webapp/frontend/src/components/Logo.jsx`, utilisé dans
le header et la page de connexion, décliné en favicon
(`public/favicon.svg`, remplace le logo Vite par défaut jamais changé).
Bug corrigé au passage : la route générique qui sert le frontend
(`app/main.py`) interceptait aussi les fichiers statiques à la racine du
build (`/favicon.svg` renvoyait le HTML de la page) — désormais un fichier
existant est servi directement (avec vérification anti path-traversal via
`is_relative_to`). Le titre d'onglet (`index.html`), resté sur "frontend"
depuis la création du projet Vite, corrigé en "MurMetric" au passage.

### Dashboard Grafana étoffé + limite mémoire InfluxDB relevée (13/08/2026)

En composant un dashboard plus complet avec l'utilisateur (température/
humidité par mur et par couche, retrait par canal, teneur en eau — 7
panels, `k8s/grafana/dashboards/hr-t-socma.json`), deux problèmes réels
découverts et corrigés :

- **Lien "Ouvrir Grafana en plein écran"** ajouté dans l'onglet (`Grafana.jsx`)
  — l'iframe embarquée reste volontairement verrouillée (mode kiosk +
  accès anonyme Viewer, cf. section précédente), donc impossible d'y
  composer ses propres graphiques ou de naviguer ailleurs. Plutôt que
  d'élever le rôle anonyme (Grafana est public sur Internet depuis
  l'ouverture du port 3000, ça ouvrirait l'édition à n'importe qui), le
  lien renvoie vers Grafana sans le mode kiosk où un compte admin donne un
  accès complet (Explore, requêtes Flux libres, nouveaux dashboards).
- **Bug de fond trouvé en composant les panels retrait : limite mémoire
  InfluxDB (1Gi) largement insuffisante pour `mesures_dewesoft`** (15 Go
  sur disque, ~1,5 milliard de points). Toute requête un peu large sur ce
  measurement faisait **OOM-kill le pod InfluxDB** (exit 137, confirmé via
  `kubectl top` juste avant le kill et `top` sur le nœud montrant 60%+
  de CPU en `iowait`) — pas un problème de syntaxe Flux ni de config
  Grafana, un problème d'allocation de ressources k8s devenu insuffisant
  après le backfill complet. Diagnostiqué en reproduisant la requête du
  panel directement via `/api/ds/query` (Grafana) puis en comparant avec
  des requêtes directes `influx query` dans le pod, en isolant
  progressivement la variable en cause (fenêtre temporelle, nombre de
  canaux combinés). `k8s/influxdb/statefulset.yaml` : limite mémoire
  256Mi/1Gi -> **512Mi/4Gi** (validé avec l'utilisateur avant application,
  ressource partagée avec MailFlow_Dylan/MINER/ollama sur le même VPS —
  un premier passage à 3Gi suffisait tout juste, ~3067Mi observés sur
  3072Mi de plafond, jugé trop proche de la limite pour être robuste dans
  la durée).
  **Deuxième correctif, plus déterminant que la mémoire seule** : combiner
  4 canaux dans un seul filtre Flux (`r.canal_nom == "HA1" or ... or
  r.canal_nom == "VA2"`) coûte disproportionnellement plus cher qu'exécuter
  4 requêtes séparées sur un seul canal chacune (vérifié empiriquement :
  la version combinée continuait de timeout/OOM même à 3-4Gi et fenêtre
  réduite, la version à 4 requêtes séparées — une par `refId` dans le même
  panel — passe en ~13s avec une marge mémoire confortable). Les 2 panels
  retrait utilisent donc désormais 4 *targets* Flux séparés (pas un filtre
  `or`) et une fenêtre par défaut réduite à **90 jours** (`"timeFrom": "90d"`
  au niveau du panel, indépendant de la période partagée du dashboard,
  1 an pour les autres panels) — cohérent avec le choix déjà fait côté
  webapp (`_FENETRE_DEFAUT_JOURS`, section précédente) pour la même raison
  exacte.

### Filtre de Hampel ajustable à la volée (13/08/2026)

En creusant une question de l'utilisateur ("brut et filtré semblent
identiques ?"), vérifié sur de vraies données que c'est **volontaire** :
`HAMPEL_SEUIL_K=8.0` (réglage d'ingestion) est délibérément conservateur —
sur une fenêtre calme de 79 minutes (canal HA1, 47 021 points), brut et
filtré sont rigoureusement identiques (écart max = 0), le filtre ne
touchant que les pics vraiment extrêmes. Sur 90 jours en revanche, l'écart
est réel : brut min -2322 mm / max 5898 mm vs filtré min -10.85 mm / max
5898 mm — le pic négatif extrême est corrigé, **le pic positif ne l'est
pas** (piste non résolue : possible rafale d'échantillons aberrants
consécutifs qui aurait faussé la médiane glissante locale, angle mort
connu d'un Hampel face à un artefact groupé plutôt qu'isolé — pas
creusé plus avant).

Cette investigation a mené à une demande explicite : le réglage
`HAMPEL_SEUIL_K`/`HAMPEL_FENETRE` (fixe, appliqué une seule fois à
l'ingestion sur le PC d'Amiens, cf. section 17) n'était pas ajustable
depuis l'interface — pour le devenir sans reprocesser les fichiers
sources, un recalcul à la volée a été ajouté :

- **`murmetric_webapp/backend/app/hampel.py`** (nouveau) — reprend
  **exactement** l'algorithme de `_filtrer_hampel_numpy()` dans
  `ingestion_dewesoft_dxd.py` (médiane + MAD glissantes, effectif =
  max(MAD des valeurs, MAD des différences successives), facteur 1,4826)
  plutôt qu'un Hampel simplifié — cohérence avec le réglage de production.
  `numpy` ajouté aux dépendances backend.
- **`GET /api/mesures/hampel`** — récupère les valeurs **brutes**
  (`valeur`, jamais `valeur_filtree`) sur une période **plafonnée à 2h**
  (mesures_dewesoft à 100 Hz — contrairement aux autres vues retrait, un
  filtre point par point ne peut pas s'appuyer sur une agrégation
  préalable, donc pas de fenêtre longue possible sans revivre le risque
  mémoire déjà rencontré) et applique le filtre avec la fenêtre/le seuil
  demandés. Ne modifie jamais `valeur_filtree` en base — recalcul pour
  l'affichage uniquement.
- **`FiltreHampel.jsx`** (nouveau, visible dans "Vue d'ensemble" pour le
  type retrait) — canal, période (max 2h), fenêtre et seuil K réglables,
  graphique SVG superposant brut (gris) et filtré-ajusté (bleu), points
  corrigés marqués en rouge.
- Validé avec de vraies données (canal HA1, 47 021 points) : K=8 (réglage
  production) → 0 aberrant sur cette période (cohérent avec l'écart nul
  déjà constaté) ; K=3 (plus sensible) → 56 aberrants détectés — le
  réglage a un effet réel et mesurable.

**Non fait à ce stade** : seul le retrait a un équivalent brut/filtré en
base (mesures_capteurs — température/humidité/point de rosée — n'a qu'un
seul champ par grandeur, cf. section précédente).

### Deuxième couche — bornes physiques absolues (13/08/2026)

Suite immédiate : l'utilisateur a comparé l'algorithme implémenté à la
définition "manuel" d'un Hampel classique (médiane + MAD simple, K=3) et a
demandé d'adopter la meilleure solution. Verdict : la version du projet
(MAD effectif = max(MAD des valeurs, MAD des différences successives),
cf. section précédente) est déjà **supérieure** à la version manuelle pour
les pics isolés — mais reste aveugle aux rafales d'échantillons aberrants
**plus longues que la fenêtre glissante** (exactement le pic +5898mm non
corrigé constaté plus haut) : dans ce cas, la médiane locale elle-même est
calculée à partir de valeurs déjà corrompues, donc rien ne semble
"aberrant" localement.

- **`appliquer_bornes_physiques()`** ajoutée à `hampel.py` — deuxième
  couche indépendante du contexte statistique local : tout point hors
  d'une plage physique absolue (`borne_min`/`borne_max`, saisie par
  l'utilisateur) est remplacé par **interpolation linéaire entre les
  voisins valides les plus proches** (pas par la médiane locale, qui
  serait tout aussi corrompue dans ce scénario). Appliquée **après** le
  Hampel, sur son résultat — rattrape ce qu'il a laissé passer.
- Endpoint `/api/mesures/hampel` et `FiltreHampel.jsx` étendus avec deux
  champs optionnels (bornes min/max) — si absents, comportement inchangé
  (Hampel seul).
- **Validé par test unitaire** avant déploiement : une rafale synthétique
  de 30 échantillons aberrants consécutifs (fenêtre Hampel=10, donc largeur
  de fenêtre 21) est **invisible pour le Hampel seul (0/30 détectés)** —
  reproduit exactement le bug réel constaté sur les données de production.
  Avec la borne physique ajoutée : **30/30 détectés et corrigés**,
  transition interpolée cohérente avec le signal environnant.
- Volontairement limité à l'outil de recalcul à la volée, **pas** au
  pipeline d'ingestion de production (`ingestion_dewesoft_dxd.py` sur le
  PC Amiens) — décision explicite de l'utilisateur, `valeur_filtree`
  stockée en base reste inchangée pour l'instant.

### Export des graphiques (PNG) — pour coller dans des rapports (13/08/2026)

Demande explicite : pouvoir copier/exporter facilement les courbes pour
les insérer dans un rapport.

- **`exportGraphique.js`** (nouveau) — deux chemins de conversion vers PNG
  selon le type de rendu de l'appli : `canvas` directement
  (`canvas.toDataURL`, utilisé par les nomogrammes 2D/3D) ou `svg` via une
  conversion (sérialisation XML → `<img>` → canvas hors-écran à 2×, utilisé
  par la courbe valeur/temps et le graphique du filtre de Hampel). Fond
  opaque ajouté explicitement à l'export SVG (`#0f1117`, même couleur que
  le thème) — sans ça l'image exportée serait transparente, illisible une
  fois collée sur un fond blanc dans un rapport.
- **`BoutonsExport.jsx`** (nouveau, réutilisable) — "Télécharger PNG" et
  "Copier l'image" (API Clipboard), ajouté sous les 4 graphiques de
  l'appli (`GraphiqueSVG`, `Nomogramme` 2D, `Nomogramme3D`,
  `FiltreHampel`).
- **Limite connue, pas encore résolue** : `navigator.clipboard.write()`
  (bouton "Copier") exige un contexte sécurisé (HTTPS) dans la plupart des
  navigateurs — l'appli tourne pour l'instant en HTTP simple
  (`http://89.168.34.201:8090`, cf. section 32 "Déployé sur le VPS"). Le
  bouton "Télécharger PNG" fonctionne dans tous les cas (pas besoin de
  l'API Clipboard) ; "Copier" échouera probablement tant que l'appli n'a
  pas de certificat TLS — pas traité ici, sujet à part (nécessiterait
  Let's Encrypt/cert-manager ou un reverse proxy TLS devant le service k3s).

### Axe Y des panels retrait borné (13/08/2026)

Suite directe de l'anomalie découverte précédemment (pic +5898mm non
filtré sur HA1) : l'utilisateur a signalé que le panel "Retrait filtré —
SOCMA 1" semblait plat. Vérifié sur données réelles — **ce n'est pas la
donnée qui est plate** : HA2/VA1/VA2 sont dans une plage tout à fait
normale (-16,3 à +2,9 mm, comparable à SOCMA 2), seul HA1 porte encore le
pic non corrigé (min -10,85 / max 5897,95 mm). Grafana règle l'axe Y du
panel pour que les 4 courbes superposées y tiennent — le pic de HA1 étire
l'échelle jusqu'à 300+, écrasant visuellement les 3 autres courbes
légitimes en une ligne plate près de zéro.

**Corrigé** (option choisie par l'utilisateur — pas de modification des
données stockées) : `fieldConfig.defaults.min/max` fixés à **-50/+50 mm**
sur les 4 panels retrait (filtré et brut, SOCMA 1 & 2) — plage
physiquement large par rapport à toute variation réelle de retrait
observée, mais bien en dessous des artefacts confirmés (±2000-6000mm).
Un artefact futur pousserait désormais la courbe en butée haute/basse du
graphique au lieu de rendre les autres courbes illisibles.

**Annulé (13/08/2026, même jour)** : l'utilisateur a demandé de restaurer
les courbes de retrait précédentes — clarifié via question explicite que
cela visait bien ce bornage -50/+50, pas une restauration de données.
`fieldConfig.defaults` remis à `{}` (auto-scale) sur les 4 panels retrait
— le dashboard est donc revenu à l'échelle automatique, le pic HA1 non
corrigé reste visible tel quel jusqu'à correction de la donnée elle-même.

### Chantier "source unique" — capteurs.json/capteurs_retrait.json (13/08/2026)

**Origine.** En réponse à une capture d'écran de la page Capteurs (tableau
"Canaux retrait" en lecture seule), l'utilisateur a demandé si rendre ces
champs (mur/couche/position) modifiables depuis la webapp serait possible,
logique, sécurisé et cohérent. Réponse donnée : techniquement possible,
mais **pas encore sécurisé** (capteurs.json/capteurs_retrait.json étaient
copiés dans l'image Docker au build, pas sur le volume persistant — un
edit webapp aurait été perdu au prochain redéploiement) ni **cohérent**
(trois copies non synchronisées existaient : dépôt git, PC Amiens/Pi
— seules copies réellement lues par l'ingestion en direct —, et image
webapp — copie d'affichage seule). Éditer depuis la webapp aurait donc été
cosmétique, sans effet sur l'étiquetage réel des mesures. Trois options
proposées (chantier complet / webapp seule avec avertissement / reporté) ;
l'utilisateur a choisi **le chantier complet : la webapp devient la source
unique de vérité, le PC Amiens et le Pi interrogent son API au lieu de
leur copie locale.**

**Backend (`murmetric_webapp/backend/app/`).**
- `config.py` : `CAPTEURS_JSON`/`CAPTEURS_RETRAIT_JSON` pointent désormais
  vers le volume persistant (`USERS_DIR=/data`, même volume que
  `users.json`) en production, toujours la racine du dépôt en dev local.
  Nouvelles constantes `CAPTEURS_JSON_SEED`/`CAPTEURS_RETRAIT_JSON_SEED`
  (copies "amorce" baties dans l'image) et `INGESTION_API_KEY` (secret
  partagé pour les endpoints machine-à-machine).
- `main.py` : `_amorcer_capteurs()` copie les fichiers "amorce" vers le
  volume persistant au tout premier démarrage seulement (volume vide juste
  après création du PVC) — jamais réécrasés ensuite, même si l'image
  change à un déploiement ultérieur.
- `routers/capteurs.py` : GET inchangés (public, utilisés par l'UI ET par
  les scripts d'ingestion). Ajout de `PUT /api/capteurs/{hr_t,retrait}/{clé}`
  (JWT requis — édition humaine des champs d'identité : nom, nom_mur,
  nom_couche, position, prestation, categorie R&D, ingestion, emplacement)
  et `POST /api/capteurs/{hr_t,retrait}/enregistrer` (protégé par l'en-tête
  `X-Ingestion-Key`, pas de session utilisateur — déclaration d'un
  canal/MAC inconnu par un script d'ingestion sans surveillance, idempotent,
  crée une entrée vide avec `ingestion: false`, miroir exact de l'ancien
  comportement local `enregistrer_canal_si_inconnu`/
  `enregistrer_capteur_si_inconnu`). Sans `INGESTION_API_KEY` configurée,
  les endpoints d'enregistrement répondent 404 (jamais exposés sans
  protection par défaut).
- `Dockerfile.webapp` : `COPY capteurs.json ./app/capteurs.seed.json` (et
  idem retrait) au lieu de copier directement sous les noms vivants.
- `k8s/webapp/deployment.yaml` : nouvelle variable `INGESTION_API_KEY`
  depuis `murmetric-secrets` (clé `webapp-ingestion-api-key`, générée par
  `openssl rand -hex 32` et poussée dans le secret via `kubectl patch`).

**Frontend.** `Capteurs.jsx` réécrit avec édition en place (même pattern
que `TeneurEau.jsx` : bouton "Éditer" → champs texte/case à cocher →
"Enregistrer"/"Annuler"). Champs éditables : nom/mur/couche/ingestion
(HR/T), mur/couche/position/ingestion (retrait) — inchangé pour les champs
non affichés (prestation, categorie R&D restent modifiables via l'API,
pas encore exposés dans le tableau, hors scope de la demande initiale).

**Distinction identité vs champs techniques BLE (Pi).** capteurs.json
mélange des champs d'identité (mur/couche/position/ingestion...) et des
champs techniques propres au Pi, écrits par `configure_capteurs.py` après
reconfiguration GATT (`lint_configure`, `lint_max_confirme_s`,
`lint_gatt_absent`, `lint_gatt_non_supporte`, `famille_capteur`,
`mac_complete_connue`, `numero_capteur_hr_t`). Seuls les champs d'identité
migrent vers la webapp — les champs techniques restent une propriété
locale du Pi, sans rapport avec le split-brain mur/couche/position qui a
motivé ce chantier. `configure_capteurs.py` n'a donc **pas été modifié**.

**Scripts d'ingestion (PC Amiens + Pi) — modifiés dans le dépôt, PAS
ENCORE déployés sur les machines distantes** (cf. point ouvert ci-dessous) :
- `ingestion_dewesoft_dxd.py` : le registre capteurs_retrait n'est plus lu
  depuis un fichier local géré à la main — récupéré via
  `GET {CAPTEURS_API_URL}/api/capteurs/retrait` (nouvelle variable d'env
  `CAPTEURS_API_URL`), rafraîchi toutes les 60s (`CAPTEURS_RETRAIT_
  RAFRAICHISSEMENT_S`, pas à chaque tour de boucle de 5s). `capteurs_retrait
  _cache.json` local sert de repli hors-ligne si l'API est injoignable
  (écrit à chaque récupération distante réussie, relu si elle échoue).
  `enregistrer_canal_si_inconnu` devient un `POST .../enregistrer`
  authentifié par `INGESTION_API_KEY` ; en cas d'échec réseau, l'entrée
  reste en mémoire pour cette exécution seulement (retentée au prochain
  démarrage, aucune mesure perdue — juste non publiée tant que non
  étiquetée).
- `ingestion_capteurs_bluetooth.py` : même principe côté HR/T
  (`GET {CAPTEURS_API_URL}/api/capteurs/hr_t`), avec la nuance champs
  techniques ci-dessus : le registre effectif en mémoire fusionne les
  champs d'identité venus de l'API avec les champs techniques relus dans
  le `capteurs.json` local (propriété de `configure_capteurs.py`). Le
  résultat fusionné est réécrit dans `capteurs.json` à chaque
  rafraîchissement — sert à la fois de cache de repli et de vue à jour
  pour `configure_capteurs.py` (exécuté séparément, jamais concurremment
  en pratique avec le script d'ingestion qui tourne en continu).
- `requirements-windows.txt`/`requirements-rpi.txt` : ajout de `requests`.
  `lancer_ingestion_dewesoft.bat.example`/`lancer_ingestion_capteurs.sh.
  example` : ajout de `CAPTEURS_API_URL`/`INGESTION_API_KEY`.

**Déployé et vérifié réel sur le VPS (13/08/2026)** : image rebuild
(`docker build -f Dockerfile.webapp`), secret `murmetric-secrets` patché
avec `webapp-ingestion-api-key` (64 caractères hex), `kubectl apply` sur
`deployment.yaml`/`pvc.yaml`, rollout restart. Vérifié en direct après
rollout :
- `/data` contient bien `capteurs.json`/`capteurs_retrait.json` (amorcés
  automatiquement depuis l'image au premier démarrage, aux côtés de
  `users.json`/`parametres.json` déjà présents).
- `GET /api/capteurs/retrait` renvoie les 8 canaux réels (HA1/HA2/VA1/VA2/
  HB1/HB2/VB1/VB2).
- `PUT /api/capteurs/retrait/VA1` (avec JWT) modifie bien la position et le
  changement est immédiatement visible au `GET` suivant — testé puis
  annulé (valeur de test posée puis restaurée à "droite").
- `POST /api/capteurs/retrait/enregistrer` sans `X-Ingestion-Key` → 404 ;
  avec la clé → crée `TEST_CANAL` (`ingestion: false`) ; rejoué à
  l'identique → idempotent (retourne la même entrée, pas de doublon) ;
  entrée de test supprimée après vérification.
- Frontend (`npm run build`) compile sans erreur ; page d'accueil et
  `/api/health` répondent 200 depuis le pod redéployé. **Non vérifié en
  navigateur réel** (pas d'outil d'automatisation navigateur disponible
  dans cet environnement) — l'UI d'édition suit le même schéma que
  `TeneurEau.jsx`, déjà en production, mais l'interaction "Éditer" n'a été
  validée qu'en simulant les appels API qu'elle déclenche, pas en cliquant
  réellement dans la page.

**Déployé sur le PC Amiens et le Raspberry Pi, avec confirmation explicite
de l'utilisateur (13/08/2026, même jour)** :
- PC Amiens (`C:\MurMetric\ingestion\`, accès SSH/paramiko via Tailscale,
  compte FRD-CODEM) : `ingestion_dewesoft_dxd.py` et
  `requirements-windows.txt` déposés par SFTP (anciennes versions
  sauvegardées en `.bak_20260813`), `lancer_ingestion_dewesoft.bat`
  réécrit avec `CAPTEURS_API_URL=http://89.168.34.201:8090` et
  `INGESTION_API_KEY` (identifiants MQTT existants préservés), `requests`
  installé. Buffer MQTT local vérifié vide avant coupure ; tâche planifiée
  `MurMetric_Ingestion_DeweSoft` arrêtée puis relancée proprement
  (`schtasks /End` + `/Run`) — reprise immédiate au fichier suivant sans
  aucun message reperdu ni rebufferisé. Log confirmé sans warning "API
  injoignable", `capteurs_retrait_cache.json` créé et à jour.
- Raspberry Pi (`murmetric-pi5`, 100.101.220.39, accès SSH via Tailscale,
  compte `murmetric`) : `ingestion_capteurs_bluetooth.py` et
  `requirements-rpi.txt` déposés (idem, sauvegardes `.bak_20260813`),
  `lancer_ingestion_capteurs.sh` réécrit avec les mêmes variables,
  `requests` installé dans le venv dédié
  (`/home/murmetric/murmetric_pi5/.venv`). Service systemd
  `murmetric-capteurs.service` redémarré (`systemctl restart`, buffer
  MQTT vérifié vide avant). Vérifié en direct : `capteurs.json` local
  réécrit par la fusion identité(webapp)/technique(local) — testé sur
  l'entrée `D2:0D:27:1C:F3:97` : champs d'identité venus de l'API,
  `lint_configure`/`lint_max_confirme_s` (propriété de
  `configure_capteurs.py`) intacts ; `📋 Registre capteurs publié sur MQTT
  (64 capteur(s))` confirme un chargement réussi depuis l'API dès le
  redémarrage.

Les deux machines distantes tournent donc désormais sur l'architecture
"source unique" — un edit fait depuis la page Capteurs de la webapp a un
effet réel sur l'étiquetage des prochaines mesures en direct (retrait :
sous 60s via le rafraîchissement périodique ; HR/T : idem). Les copies
locales (`capteurs_retrait_cache.json` sur le PC Amiens,
`capteurs.json` sur le Pi) ne sont plus que des caches de repli hors-ligne
(+ champs techniques BLE pour le Pi).

### Monitoring des pipelines d'ingestion (13/08/2026, même jour)

**Origine.** Après avoir vérifié en direct (SSH) que les deux pipelines
tournaient bien suite au chantier "source unique", l'utilisateur a demandé
que cette vérification soit intégrée à l'appli plutôt que de dépendre d'un
contrôle manuel — "m'assurer en tout temps de la santé et de l'évolution
des pipelines". Trois options présentées (fraîcheur seule / fraîcheur +
battements de vie MQTT / stack de supervision externe type
Prometheus-Grafana) ; l'utilisateur a choisi les deux premières combinées.

**Contrainte structurante identifiée avant de concevoir la solution** : la
webapp (pod k8s sur le VPS) n'a **aucun accès réseau direct** au PC Amiens
ni au Pi — seul Claude, via Tailscale, peut s'y connecter en SSH. Le seul
canal que ces machines ont déjà vers le VPS est MQTT (TLS, port 8883
externe). Toute supervision doit donc soit se déduire des données déjà
dans InfluxDB, soit transiter par ce canal MQTT existant.

**Architecture retenue** :
- **Fraîcheur** : nouvel endpoint `GET /api/monitoring/etat`
  (`routers/monitoring.py`) qui interroge InfluxDB pour le dernier point
  réellement écrit, **seulement pour les canaux/capteurs avec
  `ingestion: true`** (réutilise `_lire_json` de `capteurs.py` via import
  différé, même pattern que `_valeurs_tags_retrait()` dans `mesures.py`) —
  un capteur jamais activé n'est pas une panne, juste rien à surveiller.
  Statut `ok`/`attention`/`critique` selon l'âge du dernier point (seuils
  différents par pipeline : retrait 18h/36h, HR/T 30h/72h — capteurs BLE
  configurables jusqu'à 24h d'intervalle de log). Statut `inactif` séparé
  si zéro source active (évite une alerte rouge permanente pour HR/T,
  actuellement sans capteur activé).
- **Battement de vie** : `ingestion_dewesoft_dxd.py` et
  `ingestion_capteurs_bluetooth.py` publient toutes les
  `HEARTBEAT_INTERVAL_S` (300s défaut) un petit JSON sur
  `frd/monitoring/heartbeat` (uptime, buffer SQLite en attente, MQTT
  connecté, dernier appel au registre API réussi ou non, nombre de
  capteurs connus) — via `publier_ou_stocker()`, donc bufferisé comme les
  vraies mesures si le cloud est temporairement injoignable (aucune
  conséquence pour un battement de vie, contrairement à un point de
  mesure). Réutilise `compter_messages_en_attente()`, déjà présent dans
  les deux scripts.
- **Réception côté webapp** : plutôt que de router ces battements par
  bridge-mqtt-kafka/Kafka (pensé pour le volume/la fiabilité des mesures
  réelles, disproportionné pour un signal à faible enjeu), nouveau module
  `monitoring_mqtt.py` — la webapp s'abonne **directement** au broker MQTT
  **interne** au cluster (`mosquitto:1883` en clair, même service que
  bridge-mqtt-kafka utilise déjà en interne, pas le 8883/TLS externe) et
  écrit chaque battement dans InfluxDB (mesure `pipeline_heartbeat`, tags
  `pipeline`/`machine`) via `influx.write_point()` (déjà utilisé pour
  teneur_eau). Démarré/arrêté dans le `lifespan` de `main.py`, connexion
  non-bloquante (un broker interne injoignable au démarrage n'empêche pas
  la webapp de démarrer). `paho-mqtt` ajouté aux dépendances backend.
  `GET /api/monitoring/heartbeats?pipeline=...&heures=...` expose
  l'historique (utilisé pour un petit graphique d'évolution du buffer
  SQLite sur 24h — une valeur qui grimpe sans redescendre signale une
  perte de connexion cloud prolongée, répond au "évolution" de la demande
  initiale).
- **Frontend** : nouvelle page "Monitoring" (onglet dédié), une carte par
  pipeline avec pastille de couleur (vert/orange/rouge/gris), dernière
  donnée reçue, sources actives, puis le détail du battement de vie
  (machine, en marche depuis, MQTT connecté, buffer en attente, registre
  API à jour) et le graphique d'évolution du buffer. Rafraîchissement
  automatique toutes les 30s.

**Déployé et vérifié réel (13/08/2026)** :
- Webapp (VPS) : rebuild + rollout, log confirmé `✅ Monitoring MQTT
  connecté, abonné à frd/monitoring/heartbeat`. Chaîne complète testée
  avec un battement de vie publié manuellement (pod `mosquitto_pub`
  jetable) : reçu, écrit dans InfluxDB, restitué par `/api/monitoring/etat`
  ET `/api/monitoring/heartbeats` — point de test supprimé après
  vérification (`influx delete`).
- PC Amiens et Pi : scripts mis à jour déployés (sauvegardes
  `.bak_20260813_monitoring`), buffer MQTT vérifié vide avant coupure,
  redémarrage propre (tâche planifiée / systemd) comme pour le chantier
  précédent. **Battements de vie réels reçus des deux machines** dans les
  secondes suivant le redémarrage — `machine: "PC-BLAIDOUDI"` (8 canaux
  connus, registre API OK) et `machine: "murmetric-pi5"` (64 capteurs
  connus, registre API OK).
- **Comportement transitoire observé et attendu** : le tout premier
  battement de chaque process affiche `mqtt_connecte: false` — il part
  avant que la poignée de main MQTT asynchrone ne se termine (quelques
  centièmes de seconde). Se corrige de lui-même au battement suivant
  (300s plus tard) une fois `_mqtt_connecte` mis à jour par le callback de
  connexion — pas un bug, juste une photo fidèle de l'instant du tout
  premier envoi. Pas corrigé (retarder le premier envoi ajouterait de la
  complexité pour un désagrément cosmétique qui se résorbe seul).

### Bug trouvé et corrigé — heartbeat retrait figé pendant l'attente d'un fichier .dxd (13/08/2026)

**Symptôme signalé par l'utilisateur** : le pipeline retrait affichait
"❌ déconnecté" en continu dans la page Monitoring, alors que les logs
côté PC Amiens montraient une connexion MQTT saine, un buffer vide et
aucune erreur.

**Cause réelle** : `attendre_fichier_stable()` boucle en interne (vérifie
la taille du fichier `.dxd` en cours toutes les `POLL_INTERVAL_DXD` = 5s,
jusqu'à 3 vérifications consécutives sans changement) et ne rend la main
que si le fichier finit par se stabiliser. Le fichier DeweSoft actif
grossit en continu jusqu'à sa rotation (~12h) : cette fonction peut donc
bloquer la boucle extérieure de `boucle_surveillance()` — où vivent
`verifier_et_recharger_capteurs_retrait()` ET `envoyer_heartbeat_si_du()`
— pendant des heures. Résultat observé : le tout premier battement envoyé
juste après un redémarrage (avec `mqtt_connecte` à sa valeur du split
second avant la fin de la connexion) restait affiché indéfiniment côté
webapp, le script étant en réalité bloqué à l'intérieur de cette attente,
jamais revenu à la boucle extérieure pour en envoyer un nouveau. Ce blind
spot existait déjà pour le rafraîchissement du registre capteurs (moins
visible : le registre change rarement), le heartbeat l'a juste rendu
manifeste.

**Corrigé** : `verifier_et_recharger_capteurs_retrait()` et
`envoyer_heartbeat_si_du()` appelés aussi à l'intérieur de la boucle de
`attendre_fichier_stable()`, pas seulement autour — sans toucher à la
logique de détection de stabilité elle-même (délicate, déjà durcie après
l'incident de backfill du 09/08/2026, cf. section "RÈGLE ABSOLUE" plus
haut). Déployé et vérifié réel : après redémarrage, le second battement
(reçu alors que le script attendait toujours le même fichier `.dxd` non
stabilisé) est bien arrivé avec `mqtt_connecte: true` — confirme que la
boucle extérieure est désormais réatteinte périodiquement même en
attente prolongée sur un fichier.

### Bug trouvé et corrigé — Assistant IA aveugle à l'humidité/point de rosée (13/08/2026)

**Origine** : l'utilisateur a remarqué que la liste déroulante "Type de
mesure" de l'Assistant IA (température/humidité groupés en une seule
option "hr_t") ne proposait pas les mêmes éléments que la liste des axes
du nomogramme (température, humidité, point de rosée, retrait filtré/brut
séparés) — question posée : la liste devrait-elle être alignée ?

**Investigation** : `calculer_statistiques()` (`mesures.py`), utilisée par
`/api/assistant/chat`, calculait `champ_principal =
_CHAMPS_PAR_TYPE[type_mesure][0]` — **toujours le premier champ du type**,
donc toujours "temperature" pour hr_t, jamais "humidite" ni
"point_de_rosee", quelle que soit la sélection affichée ou la question
posée par l'utilisateur. La liste déroulante coarse n'était donc pas le
vrai problème : même parfaitement alignée sur le nomogramme, ajouter un
sélecteur de grandeur n'aurait rien changé tant que le backend ignorait
tout champ au-delà du premier.

**Corrigé** : `calculer_statistiques()` calcule désormais les 4 agrégats
(min/max/moyenne/nombre de points) pour **toutes** les grandeurs du type
sélectionné en parallèle (`champs`, pas un champ unique) — coût
négligeable (hr_t/teneur_eau : volumes instantanés ; retrait : seulement 2
champs). Nouvelle forme de réponse : `{"champs": {"temperature": {...},
"humidite": {...}, "point_de_rosee": {...}}, "mur", "couche", "debut",
"fin"}` au lieu d'un objet plat à un seul champ. `/api/mesures/statistiques`
(même fonction) n'est utilisé nulle part côté frontend en dehors de
l'assistant — changement de forme sans risque de régression visuelle.
Prompt système et description de l'outil Groq mis à jour en conséquence.
**Décision sur la liste déroulante** : gardée coarse (hr_t/retrait/
teneur_eau), pas alignée sur le catalogue d'axes du nomogramme — puisque
l'assistant voit maintenant TOUTES les grandeurs du type en une fois, un
sélecteur de grandeur individuelle serait redondant, et le regroupement
aide même l'assistant à raisonner sur les relations entre grandeurs liées
(ex. expliquer le point de rosée nécessite déjà température ET humidité).
**Vérifié réel** (VPS, après rebuild/rollout) : `GET
/api/mesures/statistiques?type=hr_t&...` renvoie bien les 3 champs avec
des valeurs cohérentes (871 points chacun) ; question posée à l'assistant
sur l'humidité (jamais accessible avant ce correctif) → réponse
`71.51331802525837`, identique au chiffre réel renvoyé par l'API.

### Assistant IA — Gemini (vision) + agrégats enrichis + nouveaux outils (13/08/2026, même jour)

**Origine.** Suite au correctif ci-dessus, l'utilisateur a demandé (1) si la
liste déroulante "Type de mesure" de l'assistant devrait être alignée sur
le catalogue d'axes du nomogramme, et (2) pourquoi seul l'agrégé est
envoyé au LLM + quelle approche innovante améliorerait l'interprétation.
Trois pistes proposées : agrégats enrichis (médiane/tendance/comparaison/
amplitude), vision (image du graphique), nouveaux outils pilotés par le
modèle. Vision d'abord écartée : **aucun modèle vision disponible sur ce
compte Groq**, vérifié par appel direct (les `llama-3.2-*-vision-preview`
répondent explicitement "has been decommissioned"). L'utilisateur a alors
fourni une clé Google AI Studio (Gemini) avec consigne : Gemini en
fournisseur primaire (texte + vision), repli automatique sur Groq pour le
texte si Gemini échoue.

**Validation technique avant implémentation** (même exigence "vérifier sur
de vraies données" que tout le reste de la session) : testé en direct
depuis le pod webapp, via le SDK `openai` déjà en place (Gemini expose une
couche de compatibilité OpenAI : `https://generativelanguage.googleapis.com/v1beta/openai/`) :
- Liste des modèles réels du compte : catalogue très différent de ce que
  je connaissais (gemini-3.x/3.5/3.6/3.7 existent déjà) — `gemini-2.5-flash`,
  `gemini-2.0-flash`, `gemini-1.5-flash` tous 404/dépréciés. `gemini-flash-latest`
  (alias toujours à jour maintenu par Google) fonctionne — retenu plutôt
  qu'un nom de version figé, justement pour éviter de reproduire l'erreur.
- Tool-calling (function calling) : fonctionne via la couche de
  compatibilité OpenAI, testé avec un outil factice (`finish_reason:
  tool_calls`, arguments correctement parsés).
- Vision : accepte un `image_url` en data URI dans le contenu du message,
  répond à une vraie question sur l'image (premier test avec une image
  PNG mal construite à la main donnait une réponse incohérente — refait
  proprement avec un PNG généré par code, réponse correcte).

**Architecture retenue :**
- `config.py` : `GEMINI_API_KEY`/`GEMINI_MODEL` (défaut `gemini-flash-latest`)/`GEMINI_BASE_URL`.
- `parametres.py`/`routers/parametres.py` : clé/modèle Gemini éditables
  depuis "Paramètres → Assistant IA" (même mécanisme que Groq : env var de
  secours, valeur persistée sur le volume prioritaire, jamais réaffichée
  en clair). Page Paramètres montre maintenant Gemini (primaire) ET Groq
  (repli) avec une note explicite sur l'ordre.
- `assistant.py` : `_completer_avec_outils(client, modele, messages)`
  factorise la boucle tool-use (bornée à 4 itérations, inchangée) —
  réutilisée pour Gemini ET Groq. `POST /api/assistant/chat` essaie
  Gemini d'abord (fil de messages neuf), et **seulement en cas
  d'exception** reconstruit un fil neuf et retente avec Groq — jamais de
  fil mixte entre fournisseurs (les messages assistant Gemini portent des
  champs propres, ex. `thought_signature`, potentiellement incompatibles
  avec Groq). Réponse inclut désormais `"fournisseur": "gemini"|"groq"`.
  Si aucun des deux n'est configuré ou que les deux échouent, message
  d'erreur listant les deux causes.
- `POST /api/assistant/chat-image` (nouveau) : Gemini exclusivement, pas
  de repli possible. Accepte `image_data_uri` + `prompt` + `selection`
  optionnelle (si fournie, les statistiques précises de cette sélection
  sont ajoutées en texte à côté de l'image, pour ancrer l'interprétation
  visuelle sur de vrais chiffres plutôt que la seule impression visuelle).
- Deux nouveaux outils exposés au modèle (option 3) : `comparer_deux_periodes`
  (deux fenêtres explicites, delta de moyenne) et `ecart_brut_filtre_retrait`
  (écart moyen absolu brut/filtré, calculé entièrement côté InfluxDB via
  `pivot()`+`map()`+`mean()`, jamais de recalcul Hampel sur une longue
  période — plafonné à 2h par ailleurs, cf. section Hampel ajustable).
- **Décision sur la liste déroulante (réponse à la question initiale)** :
  gardée coarse (hr_t/retrait/teneur_eau), pas alignée sur le catalogue du
  nomogramme — l'assistant voit maintenant TOUTES les grandeurs du type en
  un seul appel (cf. correctif précédent), un sélecteur de grandeur
  individuelle serait redondant.

**Agrégats enrichis (`calculer_statistiques()`)** — par champ, en plus de
min/max/moyenne/nombre_points déjà présents :
- `mediane` (agrégat natif InfluxDB `median()`).
- `tendance` : moyenne 1ère vs 2e moitié de la période (pas une régression
  linéaire — comparaison volontairement simple de 2 agrégats natifs, pas
  de rapatriement de série + calcul Python).
- `amplitude_jour_nuit` (hr_t uniquement) : moyenne "jour" (8h-19h UTC) vs
  "nuit" (20h-7h UTC, union de deux `hourSelection()` car pas de bouclage
  natif à travers minuit) — approximé en heures UTC, pas de conversion de
  fuseau (décalage 1-2h selon saison par rapport à l'heure locale
  d'Amiens, acceptable pour un indicateur, pas une donnée précise).
Toutes les requêtes (par champ × par enrichissement) parallélisées sur un
seul pool de threads, comme l'existant.

**Deux bugs trouvés et corrigés PENDANT la vérification en direct** (même
rigueur "vérifier sur de vraies données" que tout le reste de la
session) :
1. `hourSelection(stop: 24)` rejeté par InfluxDB ("stop must be between 0
   and 23") — bornes ajustées à jour=[8,19]/nuit=[20,23]∪[0,7] (12h/12h,
   sans chevauchement).
2. **Bug de fond, plus important, découvert en déboguant l'amplitude
   jour/nuit** : une valeur "moyenne_jour" de temperature à 0,16°C (sous
   le minimum documenté de 7,7°C — physiquement impossible) a révélé que
   `_valeur_agregat()` (donc TOUS les agrégats de `calculer_statistiques()`
   — min/max/moyenne/médiane/count, pas seulement l'amplitude jour/nuit)
   ne lisait que la PREMIÈRE table renvoyée par InfluxDB et ignorait
   silencieusement les suivantes. Or une sélection mur+couche sans
   `position` peut recouper plusieurs capteurs physiques distincts (ex.
   "SOCMA 1"+"interface carreau et exterieur" recoupe 2 capteurs à des
   positions différentes, vérifié en direct : comptages 435 et 889 sur des
   tables séparées) — les stats de l'assistant étaient donc silencieusement
   calculées sur UN SEUL capteur arbitraire sur N dès que la sélection
   n'était pas assez précise, **bug préexistant à cette session**, jamais
   détecté faute de sélection testée avec plusieurs capteurs partageant
   mur+couche. `executer_requete()` (courbe affichée dans l'appli) n'a
   jamais eu ce problème : elle itère déjà toutes les tables.
   **Corrigé** : `|> group()` ajouté avant chaque agrégat (fusionne
   toutes les tables restantes en une seule avant min/max/mean/median/
   count), dans `_valeur_agregat()` et dans `_amplitude_jour_nuit()`.
   **Effet secondaire découvert en revérifiant** : `nombre_points` pour la
   sélection de test passe de 871 (un seul capteur) à 2647 (les deux
   combinés) et révèle une **température minimale de -38,6°C** sur le
   second capteur — implausible physiquement, signature probable d'un
   artefact de mesure du même type que le pic +5898mm trouvé sur HA1
   (retrait) plus tôt dans la session. **Non corrigé à ce stade** (juste
   documenté ici) — à traiter si l'utilisateur le demande, même logique
   que pour l'anomalie HA1.

**Vérifié réel de bout en bout (VPS, 13/08/2026)** :
- Stats enrichies : médiane/tendance/amplitude_jour_nuit peuplées avec des
  valeurs cohérentes après les deux correctifs ci-dessus.
- Chat texte via Gemini : question sur la médiane → réponse exacte
  (`12,9 °C`), `"fournisseur":"gemini"` confirmé dans la réponse.
- `comparer_deux_periodes` : comparaison juin/juillet 2026 → moyennes et
  delta cohérents (juillet plus chaud, plausible saisonnièrement).
- `ecart_brut_filtre_retrait` : testé sur une fenêtre sans donnée récente
  (3 dernières heures — le pipeline retrait attend alors le fichier .dxd
  du jour, cf. section Monitoring) → requête réellement rapide (0,355s en
  direct sur InfluxDB) mais LLM qui insiste/retente sur un résultat vide,
  d'où un délai HTTP trompeur ; retesté sur une fenêtre avec données
  réelles (12/08/2026 08h-10h UTC) → réponse cohérente (écart quasi nul,
  cohérent avec l'investigation "retrait brut/filtré quasi identiques" de
  la veille).
- Repli Groq : déclenché une fois par un vrai 503 Gemini ("high demand")
  pendant les tests — confirme que le mécanisme fonctionne. Dans ce essai
  précis, Groq a lui-même échoué sur `ecart_brut_filtre_retrait`
  spécifiquement (le modèle a mal formé son propre appel d'outil,
  `tool_use_failed` côté Groq) — limite connue de certains modèles Llama
  sur des schémas d'outils plus récents/complexes, hors de mon contrôle ;
  le message d'erreur combiné (Gemini + Groq) a été retourné proprement,
  pas de crash silencieux.
- Vision : testé avec un PNG 40×40 rouge généré par code (le tout premier
  essai avec un PNG construit à la main en hex était mal formé, réponse
  incohérente — refait proprement) → réponse "Rouge" correcte.

### Sélecteur "Grandeur" séparé — température/humidité/point de rosée/retrait/teneur en eau (14/08/2026)

**Origine.** Retour à la question initiale de la veille sur l'alignement
du sélecteur avec le catalogue d'axes du nomogramme — cette fois demande
explicite et sans ambiguïté : séparer chaque grandeur plutôt que les
regrouper par type hr_t/retrait/teneur_eau dans "Type de mesure".

**Découverte en creusant** : le problème n'était pas que cosmétique.
`VueEnsemble.jsx`/`Assistant.jsx` fixaient le champ tracé via une table
`CHAMP_PRINCIPAL` figée (`hr_t`→"temperature" toujours, `retrait`→
"valeur_filtree" toujours) — **il n'existait donc aucun moyen de tracer
une courbe d'humidité ou de point de rosée dans la Vue d'ensemble ou
l'Assistant**, alors même que `/api/mesures` renvoie déjà les 3 champs
hr_t (vérifié : `GraphiqueSVG` les reçoit tous mais n'affichait que celui
imposé par la table figée). Seul le nomogramme (2D/3D) permettait déjà de
choisir la grandeur librement, via son propre catalogue `AXES_GRANDEURS`.

**Corrigé — réutilisation du catalogue nomogramme plutôt qu'une nouvelle
liste parallèle :**
- `nomogrammeAxes.js` : nouvel export `GRANDEURS_MESURABLES` =
  `AXES_GRANDEURS` (température/humidité/point de rosée/retrait filtré/
  retrait brut, catalogue nomogramme **inchangé**) + teneur en eau.
  Teneur en eau volontairement exclue du catalogue nomogramme partagé
  (données éparses saisies manuellement, aucun sens à la croiser avec des
  séries denses hr_t/retrait) mais doit rester choisissable seule dans
  Vue d'ensemble/Assistant — d'où une liste séparée plutôt qu'une
  extension du catalogue nomogramme.
- `SelecteurMesure.jsx` : "Type de mesure" (3 options groupées) remplacé
  par "Grandeur" (6 options séparées, valeurs/libellés identiques au
  nomogramme pour les grandeurs communes). `type` (dont le backend a
  besoin) et `champ` (dont `GraphiqueSVG` a besoin) sont dérivés
  automatiquement de la grandeur choisie et portés dans l'objet
  `selection` partagé.
- `VueEnsemble.jsx`/`Assistant.jsx` : table `CHAMP_PRINCIPAL` supprimée,
  `GraphiqueSVG` reçoit directement `selection.champ`.
- **Aucun changement backend nécessaire** : `champ` voyage dans les
  requêtes (`/api/mesures?...&champ=...`, `selection` envoyée à
  l'assistant) mais n'est déclaré nulle part côté backend — ignoré
  silencieusement par FastAPI (query param non déclaré) et Pydantic
  (`extra="ignore"` par défaut sur `Selection`), vérifié en direct
  (aucune erreur, comportement inchangé pour les champs déjà gérés).
  `calculer_statistiques()` continue de renvoyer TOUTES les grandeurs du
  type (pas seulement celle sélectionnée dans le picker) — la sélection
  fine ne restreint que ce qui est TRACÉ visuellement, pas ce que
  l'assistant reçoit en contexte, pour ne pas revenir sur le correctif de
  la veille.
**Vérifié réel (VPS)** : `GET /api/mesures?type=hr_t&champ=humidite&...`
renvoie bien les 3 champs (température/humidité/point de rosée, 7941
points) comme avant — `GraphiqueSVG` filtre désormais côté client sur la
grandeur choisie, rendant l'humidité et le point de rosée effectivement
traçables pour la première fois dans la Vue d'ensemble/l'Assistant.

### Bug trouvé et corrigé — "SOCMA 2" invisible dans le champ Mur (14/08/2026)

**Symptôme signalé par l'utilisateur** : "SOCMA 2" n'apparaît pas dans la
liste déroulante du champ Mur. Vérifié en direct côté API
(`/api/mesures/valeurs-tags?type=hr_t`) : SOCMA 2 est bien présent, 25
combinaisons mur/couche/position réelles avec des centaines à quelques
milliers de points chacune — **ce n'était pas un problème de donnée**.

**Cause réelle** : "Mur" était un `<input list>` (autocomplete natif),
pré-rempli avec "SOCMA 1" par défaut — la plupart des navigateurs filtrent
les suggestions de la `<datalist>` sur le texte déjà présent dans le
champ, masquant donc "SOCMA 2" tant que le champ n'était pas vidé
manuellement. Comportement natif du navigateur, pas un bug côté appli,
mais une vraie régression de découvrabilité par rapport à un menu
déroulant classique.

**Corrigé** : "Mur" passé en `<select>` strict (comme "Grandeur") — seules
2 valeurs stables et propres ("SOCMA 1"/"SOCMA 2"). Déployé et vérifié
réel (health check OK après rollout).

### Bug trouvé et corrigé — "toutes les couches" invisibles dans l'Assistant (14/08/2026, même jour)

**Symptôme** : même question que pour Mur, cette fois côté "Couche" dans
l'Assistant IA — "pourquoi toutes les couches disponibles ne sont pas
présentes ?"

**Cause réelle, pire que pour Mur** : l'état initial de `Assistant.jsx`
préremplissait `couche: "carreau_ext"` — une valeur qui **n'existe même
pas en base** (les vraies valeurs sont du texte libre du type "interface
carreau et exterieur", "milieu isolant"... cf. découverte du 12/08/2026,
section 32). `carreau_ext` était resté un ancien nom canonique jamais
nettoyé de cet état par défaut, bien après que le placeholder trompeur
équivalent ait été corrigé ailleurs. Avec un `<input list>`, ce texte
prérempli invalide filtrait la `<datalist>` sur une chaîne qu'aucune
vraie couche ne contient — masquant la totalité des suggestions, pas
seulement une.

**Corrigé** : valeur par défaut de `couche` vidée dans `Assistant.jsx`
(`""` au lieu de `"carreau_ext"`) ; "Couche" passée en `<select>` strict
comme "Mur", cette fois avec une option "— toutes —" explicite (Couche
reste un filtre optionnel, contrairement à Mur) — même décision que pour
Mur : un `<select>` ne peut structurellement pas reproduire cette classe
de bug (toujours toutes les options, jamais filtrées sur le texte
courant), la casse incohérente en base cesse d'être un problème puisque
le menu affiche les valeurs telles qu'elles existent réellement, plus
besoin de deviner l'orthographe en tapant. Déployé et vérifié réel (health
check OK après rollout).

### En-tête de courbe : libellé de la grandeur au lieu du code type (14/08/2026, même jour)

**Symptôme** : le titre au-dessus du graphique dans l'Assistant IA
affichait "Courbe — hr_t" pour une courbe de température — `hr_t` est le
code technique du TYPE (regroupant température/humidité/point de rosée),
pas la grandeur réellement affichée.

**Corrigé** : `libelleGrandeur()` (`nomogrammeAxes.js`) élargie pour
chercher dans `GRANDEURS_MESURABLES` (superset avec teneur en eau) plutôt
que `AXES_GRANDEURS` seul — reste correcte pour ses appelants existants
(nomogramme, jamais de teneur en eau) et devient utilisable hors
nomogramme. `Assistant.jsx`/`VueEnsemble.jsx` : en-tête remplacé par
`libelleGrandeur(`${selection.type}:${selection.champ}`)` → "Courbe —
Température (°C)" au lieu de "Courbe — hr_t" (Assistant) / "Courbe —
Température (°C)" au lieu du générique "Courbe valeur/temps" (Vue
d'ensemble, même amélioration appliquée par cohérence). Déployé et
vérifié réel (health check OK après rollout).

### Bug trouvé et corrigé — courbes "cassées" par un saut diagonal entre deux capteurs (14/08/2026, même jour)

**Origine.** L'utilisateur a partagé le diagnostic de l'assistant IA en
mode vision sur une courbe de température affichant deux "phases"
distinctes reliées par de longs traits diagonaux traversant le graphique.
L'assistant avait correctement repéré le symptôme (rupture d'ordre
chronologique) mais mal identifié la cause exacte ("tri manquant en
base"/"deux séries concaténées sans tri"). Demande explicite de
l'utilisateur : vérifier si ces recommandations sont déjà prises en
compte dans le code.

**Vérification → vraie cause trouvée** : `sort(columns: ["_time"])` était
bien présent dans les 3 requêtes Flux concernées (`construire_requete_flux`,
`croisement`, `_requeter_axe`) — mais **Flux trie CHAQUE TABLE
séparément, jamais entre elles**. Sans `position`/`canal_nom` précisé,
une sélection mur+couche peut recouper plusieurs capteurs physiques
distincts (ex. "SOCMA 1"+"Milieu carreau" recoupe 2 capteurs
D25DDE1B/F5D29921 à des positions différentes, vérifié en direct — même
famille de découverte que le bug `group()` des statistiques la veille).
`executer_requete()` concatène les tables telles quelles (table 0 en
entier puis table 1 en entier) : la courbe saute d'un coup du dernier
point du capteur A au premier point du capteur B — exactement l'artefact
visuel diagnostiqué par l'assistant, causé par une raison différente et
plus subtile que ce qu'il avait supposé.

**Trois requêtes corrigées** (même mécanisme que le correctif des
statistiques du 13/08/2026, appliqué ici au tracé/croisement) :
- `construire_requete_flux()` (courbe `/api/mesures`, `GraphiqueSVG`) :
  `group(columns: ["_field"])` ajouté avant `sort()` — fusionne les
  capteurs au sein d'une même grandeur (température/humidité/... restent
  séparées).
- `croisement()` (nomogramme 2D/3D, croisement de champs d'une même
  mesure) : même `group(columns: ["_field"])` ajouté **avant**
  `aggregateWindow()` — sans lui, `pivot()` opère table par table (une par
  capteur) et produit des lignes dupliquées par horodatage au lieu d'une
  trajectoire unique.
- `_requeter_axe()` (nomogramme "croisement-libre", composition
  multi-mesures) : `group()` nu ajouté (un seul champ déjà filtré ici) —
  sans lui, le dict `{horodatage: valeur}` écrasait silencieusement la
  valeur d'un capteur par celle de l'autre à chaque horodatage partagé,
  au lieu d'une vraie moyenne combinée. Bug plus sournois que les deux
  précédents : pas de rupture visuelle spectaculaire, juste des valeurs
  ponctuellement fausses/incohérentes selon quel capteur "gagnait".
`hampel()` et `ecart_brut_filtre()` non concernés : `canal_nom` y est
systématiquement précisé (retrait, un canal = un capteur physique unique,
pas d'ambiguïté possible).

**Vérifié réel (VPS)**, avant ET après déploiement :
- Requête Flux testée directement sur InfluxDB avant tout changement de
  code : confirmé 2 tables distinctes pour "SOCMA 1"+"Milieu carreau"
  (896 et 632 points), fusion propre en 1528 points correctement
  entrelacés chronologiquement après `group(columns: ["_field"])`.
- Après déploiement : `GET /api/mesures?...` renvoie les 1528 points avec
  **0 point hors ordre chronologique** (vérifié par comparaison
  horodatage par horodatage) — la courbe ne devrait plus jamais présenter
  ce saut diagonal, quelle que soit la sélection mur+couche choisie.
- `GET /api/mesures/croisement?...` (nomogramme) revérifié après
  déploiement : 1528 points, valeurs x/y cohérentes.

### Réponses de l'assistant IA sans syntaxe Markdown (14/08/2026, même jour)

**Origine.** Deuxième question sur le même échange : pourquoi le texte
généré affiche `**`, `$`... (syntaxe Markdown brute, jamais rendue par le
frontend qui affiche le texte tel quel) — l'utilisateur souhaite un texte
qui ne "ressemble pas à une IA".

**Corrigé** : `_SYSTEME` et `_SYSTEME_VISION` (`assistant.py`) complétés
d'une règle explicite interdisant toute syntaxe Markdown (gras, titres #,
listes à puces, lignes horizontales ---, notation $...$) — prose
naturelle façon note technique/email, y compris en mode "report"
(structuré par paragraphes et intitulés en texte simple suivis de
deux-points, pas de titres Markdown). Pas de rendu Markdown ajouté côté
frontend : l'instruction au modèle règle le problème à la source plutôt
que d'habiller après coup un texte encore truffé de mise en forme
"visiblement IA" (bien formaté ≠ ce qui était demandé). Limite connue :
une instruction de style n'est jamais garantie à 100% suivie par un LLM —
à surveiller, pas absolu.

**Vérifié réel** : question posée à l'assistant sur la courbe SOCMA 1 +
Milieu carreau (celle-là même utilisée pour diagnostiquer le bug de tri
ci-dessus) → réponse en prose naturelle, aucune syntaxe Markdown,
chiffres cohérents avec les statistiques réelles (moyenne 12,1°C,
amplitude jour/nuit 0,18°C, tendance +1,95°C entre les deux moitiés de
la période).

### Indicateur de chargement manquant sur "Charger la courbe" (Assistant IA, 14/08/2026, même jour)

Le bouton "Charger la courbe" de l'Assistant IA n'avait aucun état de
chargement (contrairement au bouton équivalent de la Vue d'ensemble, déjà
correct) — clic silencieux, aucun retour visuel pendant la requête.
Corrigé : nouvel état `enCoursCourbe` (séparé de `enCours`, déjà utilisé
pour l'envoi du chat, pour ne pas mélanger les deux indicateurs), bouton
désactivé + texte "Chargement..." pendant la requête, même pattern que
Vue d'ensemble. Déployé et vérifié (health check OK après rollout).

### Bug trouvé et corrigé — accents et apostrophes perdus en mode vision (14/08/2026, même jour)

**Symptôme signalé par l'utilisateur** : le texte généré par l'assistant
en mode "Envoyer avec le graphique" (vision) manque systématiquement
d'apostrophes ("L analyse" au lieu de "L'analyse") — capture d'écran à
l'appui montrant que ce n'était pas qu'un artefact de copier-coller.

**Investigation** : `GET /api/assistant/chat` (texte seul) testé en
direct — apostrophes ET accents corrects, confirmé au niveau des octets
bruts renvoyés par l'API. `POST /api/assistant/chat-image` (vision)
testé ensuite spécifiquement (le 📎 dans la capture de l'utilisateur
indiquait ce chemin, pas le chat texte) — reproduit à l'identique :
**0 apostrophe, accents systématiquement remplacés par la lettre nue**
("presente" au lieu de "présente", "donnees" au lieu de "données").
Code applicatif innocenté : `chat_image()` ne fait aucune transformation
de texte entre la réponse Gemini et le `return` — le problème se situe
dans la génération elle-même, spécifique au mode vision (le chemin texte
seul n'est jamais affecté).

**Corrigé** : `_SYSTEME_VISION` renforcé avec une instruction explicite
imposant un français complet (tous les accents, toutes les apostrophes),
en plus de l'interdiction de syntaxe Markdown déjà en place. Limite
connue, comme pour l'interdiction du Markdown : une instruction de style
n'est jamais garantie à 100% suivie par un LLM.

**Vérifié réel** : même test image répété après déploiement → réponse
avec 5 apostrophes et 10 lettres accentuées correctement formées, plus
aucune perte constatée.

### Bug trouvé et corrigé — erreur de compilation Flux en cascade avec les champs Début/Fin (14/08/2026, même jour)

**Symptôme signalé par l'utilisateur** : message d'erreur "Requête
InfluxDB échouée : compilation failed..." dès qu'une période était
choisie via les champs Début/Fin (ex. 01/06/2026 → 13/08/2026).

**Cause** : `<input type="date">` produit une date sans heure ni fuseau
(ex. "2026-06-01"). `datetime.fromisoformat()` la parse en datetime
**naïf** (sans tzinfo) ; son `.isoformat()` ne se termine ni par "Z" ni
par un offset — le littéral temporel interpolé dans la requête Flux
(`range(start: 2026-06-01T00:00:00, stop: ...)`, sans fuseau) n'est pas
reconnu par le parseur Flux, provoquant une cascade d'erreurs de
compilation. Reproduit en direct avec les valeurs exactes de
l'utilisateur avant tout correctif.

**Corrigé** : `_valider_bornes()` (`mesures.py`, partagée par
`/api/mesures`, `/api/mesures/croisement` et l'assistant IA — un seul
point de correction pour tous les chemins concernés) fixe désormais
explicitement `tzinfo=timezone.utc` sur toute date/heure naïve avant
sérialisation.

**Vérifié réel** : requête exacte de l'utilisateur (01/06/2026 →
13/08/2026, SOCMA 1 + Milieu carreau + température) → 200 OK, 0 point
(vérifié séparément par une requête directe sur InfluxDB : la dernière
vraie donnée pour ce mur+couche date du 23/03/2026, bien avant la période
demandée — le "0 point" est donc correct, pas un signe d'un problème
résiduel).

### Message explicite quand une sélection ne renvoie aucune donnée (14/08/2026, même jour)

Suite directe du correctif ci-dessus : le cas "0 point" qu'il vient de
démasquer était jusque-là **silencieux** — `VueEnsemble.jsx`/`Assistant.jsx`
n'affichaient la carte "Courbe" que si `points.length > 0`, donc un
résultat vide ne montrait tout simplement rien, sans explication.

**Corrigé** : `points` distingue maintenant explicitement "pas encore
chargé" (`null`, état initial) de "chargé mais vide" (`[]`) — un message
"Aucune donnée pour cette sélection sur la période choisie..." s'affiche
désormais dans ce second cas, dans les deux pages. Le nomogramme
(`Nomogramme.jsx`/`Nomogramme3D.jsx`) avait déjà ce traitement depuis sa
conception initiale, seules les deux pages de courbe principale
manquaient ce message. Déployé et vérifié (health check OK après
rollout).

### Suivi de l'espace disque InfluxDB (14/08/2026, même jour)

**Origine.** Après le monitoring des pipelines, l'utilisateur a demandé
si le nombre de lignes par mesure pouvait être suivi dans le temps —
analyse validée (un débit qui grimpe/stagne détecterait une dégradation
partielle qu'une simple fraîcheur ne verrait pas), mais **pas encore
implémenté** (options A "compteur ajouté au heartbeat" + B "comptage
InfluxDB borné dans le temps" proposées et discutées, en attente d'un
"quittus" explicite de l'utilisateur avant tout code — rien fait à ce
stade). Question complémentaire posée dans la foulée : suivre l'évolution
de l'**espace disque** InfluxDB (différent d'un nombre de points :
compression/index font que ce n'est pas proportionnel). Option A retenue
et implémentée immédiatement (approuvée explicitement) : vérification
périodique légère, résultat écrit dans InfluxDB lui-même.

**Contrainte de conception** : la webapp tourne dans un pod séparé de
celui d'InfluxDB, sans accès à son système de fichiers — lui donner cet
accès (exec dans un autre pod) demanderait des permissions RBAC plus
larges que nécessaire pour ce besoin.

**Implémenté** :
- `scripts/verifier_espace_disque_influxdb.sh` (nouveau, versionné dans
  le dépôt) : `kubectl exec influxdb-0 -- du -sb /var/lib/influxdb2`
  (chemin du volume confirmé en direct, ~15 Go mesurés — cohérent avec
  l'estimation précédente de la session) puis `kubectl exec influxdb-0
  -- influx write` du résultat dans la mesure `disk_usage_bytes`
  (tag `host=influxdb-0`), même bucket que le reste (`Test_Capteurs`,
  comme les battements de vie).
- **Installé en tâche cron sur le VPS** (crontab de l'utilisateur
  `ubuntu`, PAS un CronJob Kubernetes — aucun composant/permission
  supplémentaire, réutilise `kubectl`/`influx` déjà disponibles sur
  l'hôte) : `0 */6 * * *`, log dans `scripts/espace_disque.log`. Aucun
  crontab existant avant cette tâche (vérifié avant d'ajouter).
- `GET /api/monitoring/espace-disque` (`monitoring.py`, nouveau) :
  historique borné (30 jours par défaut) + dernière valeur.
- Nouveau panneau "Espace disque InfluxDB" sur la page Monitoring
  (dernière mesure formatée en Go + courbe d'évolution, même style que
  le graphique du buffer SQLite déjà présent).

**Vérifié réel** : script exécuté manuellement une première fois avant
d'installer le cron (15 942 526 193 octets, confirmé écrit dans
InfluxDB par une requête directe) ; endpoint `/api/monitoring/espace-
disque` revérifié après déploiement de la webapp → renvoie bien ce même
point. Un seul point pour l'instant (première mesure) — la courbe
d'évolution se peuplera au fil des exécutions du cron (toutes les 6h).

### Suivi du débit d'ingestion — options A+B (14/08/2026, même jour)

**Origine.** Suite directe du suivi d'espace disque ci-dessus : accord
explicite de l'utilisateur ("OK pour le suivi de débit a+b") pour les deux
options proposées, complémentaires plutôt que redondantes — A capture le
débit "vu par le process" (y compris ce qui part au buffer local), B
capture le débit "vu par InfluxDB" (la vérité terrain, indépendante d'un
bug éventuel côté process).

**Implémenté** :
- **Option A — compteurs cumulés dans le heartbeat.**
  `ingestion_dewesoft_dxd.py` réutilisait déjà deux compteurs module-level
  jamais remis à zéro (`_nb_publies`/`_nb_bufferises`) pour son log de fin
  de fichier — simplement ajoutés au payload de `envoyer_heartbeat_si_du()`.
  `ingestion_capteurs_bluetooth.py` n'avait pas cette paire : ajoutée
  (nouveaux compteurs module-level, `publier_ou_stocker()` passé de `-> None`
  à `-> bool` pour signaler succès MQTT vs bufferisation SQLite, site
  d'appel dans `callback()` incrémente en conséquence), puis exposée dans
  `tache_heartbeat()`.
- **Option B — comptage InfluxDB borné dans le temps.** Nouvelle fonction
  `_points_recus_fenetre()` (`monitoring.py`) : `count()` Flux sur une
  fenêtre de 24h (défaut), tous canaux/capteurs actifs combinés en un seul
  filtre `or` — performance testée en direct avant d'écrire le code final
  (8 canaux retrait combinés, 24h : 0,65 s, 6,3M points, largement dans la
  marge malgré la mise en garde connue sur `mesures_dewesoft` avec filtres
  `or` combinés — cf. Historique 13/08/2026, "OOM InfluxDB" — cette
  requête-ci reste un simple `count()`, pas le cas coûteux identifié
  alors). Résultat exposé comme `points_24h` dans `GET /api/monitoring/etat`
  pour chaque pipeline.
- Page Monitoring (`Monitoring.jsx`) : nouveau champ "Points reçus (24h,
  InfluxDB)" (option B) et "Points publiés / bufferisés (cumul process)"
  (option A) dans chaque carte pipeline.

**Bug trouvé et corrigé en vérifiant** (même famille que les précédents de
cette session : ne jamais déclarer "fait" sans repasser par de vraies
données) — après déploiement des deux scripts d'ingestion et de la webapp,
`GET /api/monitoring/etat` renvoyait `points_24h` correctement (option B,
données réelles confirmées : 6,27M points retrait/24h) mais
`nb_points_publies`/`nb_points_bufferises` restaient `null` malgré le
nouveau code déployé côté process. Cause : le battement de vie ne transite
**pas** par le pipeline Kafka habituel (volontairement, cf. commentaire
`monitoring_mqtt.py`) — il est reçu directement par un abonné MQTT interne
à la webapp (`monitoring_mqtt.py`, broker `mosquitto:1883`) qui
reconstruit lui-même la ligne Influx à écrire à partir d'une **liste de
champs codée en dur** (`_traiter_message()`), sans jamais avoir été mise à
jour pour les deux nouveaux champs — ils étaient donc bien publiés sur
MQTT par les deux scripts, mais silencieusement ignorés à l'écriture dans
InfluxDB. Corrigé (`nb_points_publies`/`nb_points_bufferises` ajoutés à la
liste), redéployé, reconfirmé avec un vrai battement reçu après le
correctif.

**Vérifié réel** :
- PC Amiens (tâche planifiée `MurMetric_Ingestion_DeweSoft`) et Raspberry
  Pi (`murmetric-capteurs.service`) redéployés avec confirmation explicite
  de l'utilisateur avant de toucher ces process de production ; buffer
  SQLite vérifié vide avant chaque redémarrage (0 ligne sur le PC Amiens
  via une requête directe sur `murmetric_buffer.db`), reprise immédiate
  confirmée dans les deux cas (nouveau PID/heure de démarrage observée).
- `GET /api/monitoring/etat` revérifié après le correctif
  `monitoring_mqtt.py`, en attendant un vrai cycle de heartbeat (300s) sur
  les deux pipelines :
  - Retrait (PC Amiens) : `points_24h: 6 232 862` (option B, InfluxDB),
    `nb_points_publies: 0`, `nb_points_bufferises: 1` sur le premier
    battement post-redéploiement (un seul message bufferisé pendant la
    fenêtre de reconnexion MQTT juste après le redémarrage — cohérent,
    `mqtt_connecte` repassé à `true` dans la foulée).
  - HR/T (Pi) : `nb_points_publies: 0`, `nb_points_bufferises: 0` — champs
    bien présents (non `null`) sur le battement post-redéploiement,
    confirmant le correctif côté `monitoring_mqtt.py` pour les deux
    pipelines. `points_24h` reste `null` pour ce pipeline, `nb_sources_
    actives: 0` — aucun capteur Blue Maestro actuellement détecté par le
    Pi (logs : "Aucun capteur Blue Maestro détecté"), condition
    préexistante sans rapport avec ce chantier, non traitée ici.
- Mot de passe SSH fourni en session (identique pour les deux comptes),
  jamais stocké.

### Mise en conformité PEP 8 + docstrings, tout le code Python + équivalent JS (14/08/2026, même jour)

**Origine.** Demande explicite de l'utilisateur : "tous les codes doivent
être bien commentés et conformes PEP 8". Portée précisée par échange :
tout le code Python existant (pas seulement les nouveaux fichiers), et un
équivalent côté JS/React (commentaires utiles + formatage cohérent).

**Outillage retenu** :
- Python : `black` (formatage automatique) + `ruff` (règles `E`/`W`
  pycodestyle, `F` pyflakes, `I` tri des imports, `D100`-`D103` docstrings
  manquantes). Limite de ligne fixée à **100 caractères**, pas les 79
  stricts de PEP 8 — le volume de commentaires en français déjà présent
  dans ce dépôt aurait rendu un retour à la ligne à 79 caractères illisible
  (79 reste une déviation possible si demandé explicitement, cf. PEP 8
  lui-même qui autorise des dérogations documentées).
- JS/React : `prettier` (nouveau, ajouté à `devDependencies` avec un
  `.prettierrc.json` — largeur 120, cohérent avec le choix de 100 pour
  Python). `oxlint` était déjà en place comme linter (`npm run lint`) et
  passait déjà à 0 erreur, réutilisé tel quel plutôt que d'ajouter ESLint
  en double.

**Réalisé** :
- Les 26 fichiers Python du dépôt (hors `DWDataReader_v5_0_8/`, SDK vendor
  tiers non touché) reformatés avec `black`, puis 0 erreur restante sur
  `ruff check --select E,W,F,I` (quelques correctifs manuels : lignes trop
  longues dans des docstrings/f-strings/descriptions FastAPI, une variable
  ambiguë `l` renommée `ligne` dans `backfill_hr_t.py`).
- ~70 fonctions/classes/modules publics sans docstring identifiés via
  `ruff --select D100,D101,D102,D103` et complétés — une ligne de résumé
  concise par élément (pas de pavé de prose), cohérent avec le style de
  commentaires déjà établi dans ce dépôt (expliquer le "pourquoi" quand il
  n'est pas évident, pas reformuler ce que le code dit déjà). Les fonctions
  privées (préfixe `_`) et les scripts autonomes ne sont pas concernés par
  cette règle (convention PEP 257 standard).
- Côté frontend : 17 fichiers reformatés avec Prettier (0 changement de
  logique). Ajout de quelques commentaires ciblés là où un fichier entier
  n'en avait aucun et contenait une logique non triviale (`App.jsx` :
  garde de route ; `Monitoring.jsx` : mise à l'échelle SVG des graphiques,
  commentaire d'en-tête de page ; `Parametres.jsx` : pourquoi un nouveau
  jeton JWT est réappliqué après modification du compte, commentaire
  d'en-tête). Les fichiers déjà bien commentés (nomogramme, sélecteur de
  mesure, export graphique) laissés tels quels — pas de sur-commentaire.
- Aucun changement de comportement : uniquement formatage + docstrings/
  commentaires. Vérifié par syntax-check (`ast.parse`) sur les 26 fichiers
  Python, build frontend (`npm run build`) + lint (`oxlint`) + vérification
  de formatage (`prettier --check`), tous passants avant déploiement.

**Déployé et vérifié réel** (avec confirmation explicite de l'utilisateur,
séparée pour le webapp et pour les process de prod PC Amiens/Pi vu que ces
derniers n'avaient aucune raison fonctionnelle d'être touchés) :
- Webapp (VPS) : rebuild image + rollout, `/api/health` → 200 OK,
  `/api/monitoring/etat` répond normalement après redéploiement.
- PC Amiens (`ingestion_dewesoft_dxd.py`) et Pi
  (`ingestion_capteurs_bluetooth.py`) : buffer SQLite vérifié vide avant
  chaque redémarrage, reprise confirmée (nouveau PID/heure de démarrage,
  heartbeat reçu après redéploiement).

### Export CSV/Excel des données (14/08/2026, même jour)

**Origine.** Demande explicite : rendre les données exportables par
l'utilisateur (CSV/Excel), pour analyse hors appli. Périmètre précisé avec
l'utilisateur : courbes de mesure (Vue d'ensemble + Assistant), tableau
teneur en eau, registres capteurs (HR/T + retrait) — les autres tableaux
(Monitoring) restent hors périmètre pour l'instant.

**Implémenté** :
- `exportDonnees.js` (nouveau) : `telechargerCSV()` (JS pur, aucune
  dépendance) et `telechargerExcel()` (SheetJS `xlsx`).
- `components/BoutonsExportDonnees.jsx` (nouveau) : deux boutons "Export
  CSV"/"Export Excel" réutilisables, même patron que `BoutonsExport.jsx`
  (export PNG déjà existant).
- Câblé dans `GraphiqueSVG.jsx` (donc `VueEnsemble.jsx` ET
  `Assistant.jsx`, qui le réutilisent tous les deux), `TeneurEau.jsx`
  (tableau des saisies) et `Capteurs.jsx` (registres HR/T + retrait).

**Décision technique — dépendance `xlsx`** : le paquet npm officiel
`xlsx` porte deux failles connues (pollution de prototype, ReDoS) sans
correctif publié sur le registre npm depuis plusieurs versions — SheetJS
distribue les correctifs uniquement via son propre CDN
(cdn.sheetjs.com) depuis qu'ils ont cessé de publier sur npm. Testé
`exceljs` comme alternative : rejeté (98 paquets ajoutés, ses propres
vulnérabilités modérées via une dépendance `uuid` obsolète — pas
réellement meilleur). Installé finalement depuis
`https://cdn.sheetjs.com/xlsx-0.20.3/xlsx-0.20.3.tgz` (méthode
officiellement recommandée par SheetJS) : `npm audit` → 0 vulnérabilité,
un seul paquet ajouté.

**Import différé (code-splitting)** : `xlsx` pèse ~330 ko compressé — son
import a été rendu dynamique (`await import("xlsx")` dans
`telechargerExcel()` uniquement) pour ne pas alourdir le chargement
initial de l'appli pour une fonctionnalité que la plupart des sessions
n'utiliseront jamais. Vérifié par le build : bundle principal inchangé
(~290 ko), `xlsx` isolé dans son propre chunk (~492 ko) chargé seulement
au clic sur "Export Excel".

**Vérifié** : build frontend + lint (`oxlint`) + formatage (`prettier`)
tous passants, bundle correctement scindé (vérifié dans la sortie de
`vite build`), déployé sur le VPS — `/api/health` 200 OK, le chunk
`xlsx-*.js` répond 200 en direct. **Non vérifié visuellement dans un
navigateur** (pas d'outil de test navigateur disponible dans cet
environnement) — le téléchargement CSV/Excel proprement dit (contenu du
fichier généré, ouverture réelle dans Excel) n'a pas été testé au-delà de
la revue de code et de la vérification que le code se charge sans erreur.

### Audit et retrait des emoji dans le code (14/08/2026, même jour)

**Origine.** Suite du chantier PEP 8/commentaires : demande de vérifier
l'absence d'emoji dans "les codes" (pas seulement l'interface web déjà
nettoyée le même jour, cf. entrée "Pastille" plus haut).

**Audit** : recherche par plage Unicode (pictogrammes, symboles divers,
dingbats) sur tous les fichiers `.py`/`.js`/`.jsx` du dépôt. 139
occurrences trouvées dans 12 fichiers Python (tous des print()/log de
supervision — aucun emoji dans la logique métier elle-même). Côté
frontend : rien à retirer au-delà du nettoyage déjà fait plus tôt dans la
session (composant `Pastille`) — seule une coche texte simple `✓` déjà
étudiée et conservée subsiste (glyphe typographique par défaut, pas un
pictogramme couleur).

**Distinction volontaire conservée** : flèches (`→`, `←`), symboles
mathématiques (`≈`, `≠`, `∪`), séparateurs de section en ASCII-art
(`━━━`) et symboles sans variante emoji forcée (`▶` simple, sans
sélecteur de variante U+FE0F) ne sont PAS retirés — ce sont des
caractères typographiques standards, pas la signature "IA" que ce
chantier cible. En revanche, les mêmes symboles AVEC le sélecteur de
variante emoji (`✔️`, `▶️`, `↩️`) ont été retirés au même titre que les
pictogrammes couleur classiques (`✅`, `❌`, `⚠️`), car ce sélecteur force
justement un rendu emoji coloré.

**Corrigé** : les 139 occurrences remplacées par du texte simple
("Attention :", "Erreur :", ou simplement retirées quand le message
restait clair sans préfixe). Au passage, 4 commentaires expliquant
`sys.stdout.reconfigure(encoding="utf-8")` par "requis pour les emoji"
corrigés — la vraie raison (accents français, toujours présents) ne
dépendait pas des emoji retirés.

**Vérifié** : `black`+`ruff` (PEP 8, docstrings) + `ast.parse` (syntaxe)
repassés sur les 12 fichiers après coup — certains remplacements avaient
allongé des lignes au-delà de 100 caractères, corrigés. Un second balayage
avec une plage Unicode plus large a trouvé 2 occurrences manquées
(`↩️`, un `⏳` isolé) au premier passage — corrigées.

**Déployé** (avec confirmation explicite de l'utilisateur, portée plus
large que d'habitude car elle touche 2 pods VPS supplémentaires
jamais reconstruits pour du pur style dans cette session) :
- PC Amiens (`ingestion_dewesoft_dxd.py`) et Pi
  (`ingestion_capteurs_bluetooth.py`, `configure_capteurs.py`,
  `start.py`) — buffer vide vérifié avant chaque redémarrage, reprise
  confirmée.
- VPS : rebuild + rollout de `kafka-consumer-influx` et
  `bridge-mqtt-kafka` (jamais reconstruits depuis leur dernier
  déploiement fonctionnel) en plus de `murmetric-webapp`. Logs des 3 pods
  vérifiés après redémarrage (démarrage propre, aucun emoji résiduel,
  aucune erreur) ; `/api/monitoring/etat` confirme les deux pipelines
  toujours opérationnels après coup (nouveaux `demarre_le`, `points_24h`
  toujours alimenté).
- `bridge_mqtt_to_influx.py` : nettoyé dans le dépôt mais **non
  redéployé** — fichier confirmé non utilisé en production (aucune
  référence dans les manifestes k8s actifs, superseded par le pipeline
  Kafka ; seul le `Dockerfile` racine legacy le référence encore, lui
  aussi hors service).

### Boutons d'export sur une seule ligne + disclaimer IA isolé (14/08/2026, même jour)

**Boutons d'export** : `BoutonsExport`/`BoutonsExportDonnees` gagnent une
prop `imbrique` (rend juste les boutons, sans wrapper/marge propre) —
`GraphiqueSVG.jsx` compose les 4 boutons (PNG/copie/CSV/Excel) sur une
seule ligne avec un séparateur vertical entre le groupe image et le
groupe données, plutôt que deux rangées qui s'empilaient de façon peu
lisible. Déployé et vérifié (`/api/health` 200 OK).

**Disclaimer IA isolé** : demande explicite de distinguer visuellement la
phrase de rappel "lecture assistée par IA"/"brouillon à valider" du reste
de la réponse de l'assistant. Deux volets :
- `_SYSTEME`/`_SYSTEME_VISION` (`assistant.py`) : instruction ajoutée pour
  que ce rappel forme toujours son propre paragraphe, commençant
  littéralement par "Note : ".
- `Assistant.jsx` : nouveau composant `TexteAssistant` — découpe la
  réponse en paragraphes (séparateur `\n\n`), détecte celui qui commence
  par "Note :" (insensible à la casse) et l'affiche en italique/discret,
  scopé aux messages `role === "assistant"` (jamais aux messages de
  l'utilisateur).

**Non vérifié en conditions réelles** : le comportement du LLM (respect
de la consigne "Note : " en début de paragraphe) n'a pas été retesté en
direct — nécessiterait un identifiant webapp authentifié et un navigateur,
aucun des deux disponible dans cet environnement. Build/lint/déploiement
vérifiés ; **à confirmer par l'utilisateur** en testant une vraie réponse
de l'assistant (mode "explain" ou "report", avec ou sans image).

### Erreur 429 (quota Gemini) affichée en brut — diagnostic + correctif du formatage (14/08/2026, même jour)

**Origine** : l'utilisateur a signalé une erreur affichée en clair dans
l'assistant lors d'une analyse d'image — le corps JSON complet de
l'erreur Gemini (dict Python imbriqué, listes, guillemets simples)
remontait tel quel jusqu'à l'interface.

**Diagnostic** : deux causes distinctes, une externe (non corrigeable),
une interne (bug de formatage, corrigé) :
- **Cause réelle de l'échec** : quota Gemini API Studio épuisé —
  compte gratuit limité à 20 requêtes/jour pour `gemini-3.7-flash`
  (`GenerateRequestsPerDayPerModelPerFreeTier`), atteint après les
  nombreux tests de la session. Limite côté compte Google, pas un bug
  applicatif — non corrigeable par du code. Le `retryDelay` de ~36s
  renvoyé par Google semble incohérent avec un quota "par jour" (peut-être
  un comportement de fenêtre glissante côté Google, pas garanti) : en cas
  de blocage persistant, les options sont d'attendre le renouvellement
  quotidien ou de passer à un plan payant sur Google AI Studio.
- **Vrai bug trouvé et corrigé** : `chat_image()` et le repli Gemini→Groq
  de `chat()` (`assistant.py`) faisaient `f"... : {exc}"` sur l'exception
  brute du SDK `openai` — pour une `APIStatusError` (429/401/...), `str()`
  inclut tout le corps de réponse JSON de l'API, illisible pour
  l'utilisateur final.

**Corrigé** : nouvelle fonction `_message_erreur_ia(fournisseur, exc)` —
détecte les erreurs typées du SDK (`APIStatusError` et ses sous-classes
`RateLimitError`/`AuthenticationError`, communes à Gemini ET Groq via
l'API compatible OpenAI) et produit un message clair par code HTTP (429 →
quota, 401 → clé invalide, autre code → générique), avec repli sur
`str(exc)` seulement pour une exception vraiment inattendue (réseau...).
Utilisée aux 3 points d'affichage d'erreur IA du fichier.

**Vérifié** : logique testée directement dans l'image Docker déployée —
confirmé que `openai.RateLimitError`/`AuthenticationError` héritent bien
de `APIStatusError` et exposent `.status_code` (lu dans le code source du
SDK installé, pas supposé), instance simulée avec `status_code=429`
donnant le message attendu. `black`/`ruff`/syntax-check passants, webapp
redéployée, `/api/health` 200 OK. **Non testé en conditions réelles** (pas
de nouvelle vraie erreur 429 déclenchée pendant la vérification, pas
d'accès navigateur) — le prochain 429 réel affichera le message propre si
le diagnostic est correct.

## 33. Chantier migration InfluxDB → TimescaleDB (17/08/2026)

**Origine.** Demande explicite de l'utilisateur : simplifier les requêtes
Grafana en remplaçant InfluxDB/Flux par TimescaleDB/SQL — motivé par une
frustration concrète et légitime, pas théorique : cette session a
justement corrigé 4 bugs distincts causés par la sémantique "par table"
de Flux (`sort()`/`pivot()`/`aggregateWindow()` n'opèrent jamais entre
tables sans `group()` explicite), dans `_valeur_agregat()`,
`construire_requete_flux()`, `croisement()` et `_requeter_axe()`.

**Plan validé avec l'utilisateur, en 6 phases**, avec comme principe
directeur qu'InfluxDB reste la source de vérité intacte et en lecture
seule jusqu'à validation complète de TimescaleDB (aucune suppression,
aucune coupure d'écriture avant la Phase 6) :
0. Conception du schéma + déploiement TimescaleDB isolé (fait, ce jour)
1. Migration de l'historique (lecture seule sur InfluxDB, vérifiée par lot)
2. Double écriture pour les nouvelles données pendant la transition
3. Réécriture du backend (`mesures.py`) + validation croisée Flux/SQL
4. Reconstruction des 7 panels Grafana
5. Bascule des lectures + période d'observation (1-2 semaines)
6. Décommissionnement (seulement après confiance totale)

### Incident en cours de Phase 0 : OOM InfluxDB pendant l'exploration du schéma

**Ce qui s'est passé** : une requête Flux `keys()` sans fenêtre temporelle
assez étroite sur `mesures_dewesoft` (1,5 milliard de points) a fait
sortir le pod InfluxDB en OOM-kill (exit 137). Auto-redémarré par k3s en
~20s (StatefulSet), aucune perte de données (volume persistant, moteur
TSM robuste aux arrêts brusques) — confirmé par la reprise normale des
deux pipelines juste après (heartbeats à jour, `points_24h` alimenté).

**Root cause** : même famille de piège que l'incident du 13/08/2026 déjà
documenté (limite mémoire InfluxDB face au volume de `mesures_dewesoft`)
— toute opération non bornée dans le temps sur cette mesure reste
dangereuse, `keys()`/`schema.measurementTagKeys()` y compris, pas
seulement les agrégations lourdes déjà identifiées.

**Correctif de méthode** : reprise immédiate avec des requêtes bornées
(`last()` sur un canal filtré plutôt que `keys()` sur toute la mesure) —
schéma des 5 mesures obtenu sans nouvel incident. **Point de vigilance
permanent pour la suite du chantier** (Phase 1 notamment, qui va lire
tout l'historique de `mesures_dewesoft`) : ne jamais interroger cette
mesure sans borne temporelle explicite, quelle que soit la fonction Flux
utilisée.

### Phase 0 réalisée — schéma vérifié + déploiement isolé

**Schéma InfluxDB vérifié sur données réelles** (pas supposé depuis la
mémoire du code d'ingestion — requêtes bornées dans le temps après
l'incident ci-dessus) :

| Mesure | Tags | Fields |
|---|---|---|
| `mesures_dewesoft` | canal_nom, canal_unite, nom_mur, nom_couche, position, rd, source | valeur, valeur_filtree (float), est_aberrant (bool), canal_index (int), taux_echantillonnage (float), horodatage_lisible (string) |
| `mesures_capteurs` | adresse_mac, emplacement, nom_capteur, nom_mur, nom_couche, position, rd | temperature, humidite, point_de_rosee (float), mac_complete_connue (bool) |
| `mesures_teneur_eau` | mur, couche, prestation, utilisateur_id, utilisateur_nom | teneur_eau_pourcent (float), commentaire (string) |
| `pipeline_heartbeat` | machine, pipeline | mqtt_connecte, registre_api_ok (bool), buffer_sqlite_en_attente, nb_capteurs_connus, nb_points_publies, nb_points_bufferises (int), demarre_le (string) |
| `disk_usage_bytes` | host | value (numeric) |

Note : le tag "categorie R&D" (espace + esperluette dans le code Python)
est en réalité écrit sous la clé `rd` côté InfluxDB — confirmé par les
données réelles, pas par le code d'ingestion.

**Implémenté** (`k8s/timescaledb/`) :
- `schema.sql` : une hypertable par mesure, précision `TIMESTAMPTZ`
  (microseconde) plutôt que la nanoseconde native d'InfluxDB — sans
  impact pour ce projet (retrait à 100 Hz = 10 000 µs entre échantillons,
  très au-dessus de la résolution microseconde). `chunk_time_interval`
  différencié par volume (6h pour `mesures_dewesoft`, 7-30j pour les
  mesures plus légères) — point de départ à retuner empiriquement une
  fois l'historique réel chargé en Phase 1, pas une valeur figée.
  Index sur les combinaisons de tags les plus filtrées (canal, mur+couche,
  MAC).
- `configmap.yaml`/`statefulset.yaml`/`pvc.yaml`/`service.yaml` : suit les
  mêmes conventions que `k8s/influxdb/` (ressources modestes au départ,
  nœud partagé — 100m/256Mi requêtés, 500m/1Gi en limite). PVC 30Gi.
  Service ClusterIP, aucune exposition externe.
- Mot de passe superuser généré aléatoirement (jamais demandé à
  l'utilisateur, jamais stocké en mémoire persistante), ajouté à
  `murmetric-secrets` (clé `timescaledb-password`) + template mis à jour.

**Vérifié réel** : les 5 hypertables créées sans erreur au premier
démarrage (`create_hypertable` confirmé pour chacune dans les logs), test
d'insertion/lecture/suppression réussi sur `mesures_dewesoft`, production
(InfluxDB, retrait, hr_t) confirmée non affectée par ce déploiement
(pipelines toujours opérationnels après coup).

### Phase 1 (historique) — tentative du 17/08/2026, mise en pause

**Ce qui s'est passé.** Premier essai du script `migration_influx_timescale.py`
(lecture InfluxDB → écriture TimescaleDB, mesure par mesure/tag/jour). Trois
incidents production le même jour, tous auto-résolus par k3s (redémarrage
automatique du pod StatefulSet), **aucune perte de données confirmée à
chaque fois** (volume persistant, moteur TSM d'InfluxDB robuste aux arrêts
brusques ; pipelines retrait/hr_t vérifiés opérationnels après coup) :

1. **`group()` sans colonnes avant `first()`/`last()`** dans
   `_premier_dernier_jour()`, ajouté par précaution sur un flux déjà filtré
   à une seule série (un seul tag + un seul field) — s'est révélé
   catastrophique plutôt qu'inoffensif : load average du VPS monté à 40+
   sur 4 cœurs, SSH lui-même injoignable ~45s, `kafka-consumer-influx`
   redémarré (timeout de la sonde de vivacité, pas OOM). Correctif : ce
   `group()` retiré — un filtre tag+field déjà univoque n'en a jamais eu
   besoin.
2. **Même requête via la librairie Python `influxdb-client` (HTTP,
   VPS-hôte → InfluxDB ClusterIP)** — chemin jamais utilisé ailleurs dans
   ce projet (tous les autres scripts interrogent InfluxDB depuis
   l'intérieur d'un pod). Mémoire InfluxDB montée à ~4095Mi (plafond
   4Gi), un essai identique suivant a fait crasher le pod. Correctif
   (autorisé explicitement) : réécriture complète du script pour ne plus
   jamais utiliser `influxdb-client` — toutes les lectures passent
   désormais par `kubectl exec influxdb-0 -- influx query ... --raw`
   (sous-processus, même chemin que le reste du projet), avec un parseur
   CSV annoté maison (`_parser_csv_annote()`).
3. Même ralentissement/instabilité persistant malgré la réécriture — deux
   renforcements supplémentaires appliqués (autorisés explicitement) :
   - **Quota mémoire par requête sur InfluxDB** (`k8s/influxdb/statefulset.yaml`) :
     `--query-memory-bytes=1073741824` (1GiB) + `--query-initial-memory-bytes=10485760`.
     Absence totale de garde-fou serveur confirmée dans les logs avant
     correctif (`memory_bytes_quota_per_query` au maximum int64, illimité
     de fait) — un vrai manque, indépendant du diagnostic ci-dessous.
     Déployé par redémarrage contrôlé (`kubectl apply` + `rollout status`),
     pas déclenché par un crash.
   - Remplacement de `range(start:0)` par une borne explicite
     (`DATE_DEBUT_HISTORIQUE = "2024-01-01T00:00:00Z"`) dans toutes les
     requêtes du script, hypothèse : un départ epoch non borné empêche
     l'élagage des shards par InfluxDB.
   - **Cause racine non tranchée avec certitude.** La même requête
     (`last()`/`first()` sur un canal+field, bornée) était rapide au tout
     premier essai du jour (1,083s), puis systématiquement lente (20s+)
     après les crashs répétés — hypothèse retenue : les redémarrages
     brutaux (SIGKILL) ont dégradé l'état interne d'InfluxDB (cache, WAL,
     retard de compaction) plutôt qu'un défaut de conception de requête,
     mais ni le retrait du `group()`, ni le passage à `kubectl exec`, ni
     le quota mémoire, ni la borne de date n'ont isolément fait
     disparaître la lenteur au moment des tests.

**Décision (accord explicite de l'utilisateur, "oui" à la mise en
pause proposée)** : Phase 1 mise en pause pour la journée sans nouvel
essai contre la production. Seul ce qui est sûr est conservé/commité
(durcissement InfluxDB, script de migration corrigé) — pas de reprise
de la migration elle-même sans un nouveau feu vert explicite.

**Vérifié réel après mise en pause** : ~10 minutes après le dernier
redémarrage contrôlé, pod InfluxDB stable (0 redémarrage), mémoire
redescendue d'elle-même à 663Mi (contre ~4095Mi au plus fort de
l'instabilité) sans nouveau crash — cohérent avec l'hypothèse d'un état
interne dégradé temporaire plutôt qu'un problème structurel permanent.
Point de vigilance pour la reprise de Phase 1 : retester la requête de
référence à froid avant de relancer une migration par lots complète.

### Phase 1 reprise le 17/08/2026 (même jour) — 4 mesures migrées, mesures_dewesoft re-bloquée

**Requête de référence retestée à froid avant toute reprise** (comme prévu
ci-dessus) : `last()` bornée, canal+field unique → 0,614s, mémoire quasi
stable (568→581Mi). Confirme l'hypothèse d'un état dégradé temporaire —
InfluxDB était revenu à la normale après la pause.

**4 mesures migrées intégralement et vérifiées, aucun incident** :
`disk_usage_bytes`, `pipeline_heartbeat`, `mesures_teneur_eau`,
`mesures_capteurs` (59/59 adresses MAC, 5888 lots, 45 098 lignes). Deux
bugs du script trouvés et corrigés au passage (jamais déclenchés
auparavant, les 3 incidents précédents portaient tous sur
`mesures_dewesoft`) :
1. `pivot()` ne conserve que les colonnes de la clé de groupe — pour les
   mesures sans `tag_decoupage` (`disk_usage_bytes`, `pipeline_heartbeat`,
   `mesures_teneur_eau`), les tags point-par-point (`host`, `pipeline`...)
   étaient silencieusement perdus avant l'écriture TimescaleDB
   (`NotNullViolation`). Corrigé : `group()` inclut désormais ces tags
   quand `tag_decoupage` est absent.
2. `schema.tagValues()` a un lookback par défaut de -30 jours sans
   paramètre `start` explicite — `_valeurs_distinctes()` aurait
   silencieusement ignoré tout capteur/canal inactif depuis plus
   longtemps (0 erreur, juste jamais migré). Découvert sur
   `mesures_capteurs` (0 adresse MAC trouvée alors que la mesure contient
   des données depuis janvier 2026, 59 adresses réelles). Corrigé avec
   `start: DATE_DEBUT_HISTORIQUE`.
   Aussi noté au passage un timeout isolé et non reproductible (20s) sur
   une requête `_tags_constants()` en plein milieu du run `mesures_capteurs`
   — mémoire InfluxDB restée basse pendant l'incident (pas de OOM), retesté
   immédiatement après à 0,245s : aléa transitoire côté `kubectl
   exec`/sous-processus, pas une dégradation InfluxDB. Le run a simplement
   été relancé (design idempotent, reprise automatique sans repasser sur
   les lots déjà vérifiés).
   Piège opérationnel noté : `commande | tee fichier.log | head -N` a tué
   prématurément le premier essai `mesures_capteurs` par SIGPIPE quand
   `head` s'est arrêté après N lignes — ne jamais pipe une commande longue
   à travers `head`/`tail` en aval d'un `tee`, rediriger directement vers
   un fichier (`> fichier.log 2>&1`) à la place.

**`mesures_dewesoft` : 2 nouveaux OOM réels, migration arrêtée après 1
seul jour migré (HA1/21-11-2025, 178 966 points, vérifié).**
1. `first()` sur HA1, même avec un timeout client remonté à 120s pour la
   laisser aller au bout (au lieu de couper à 20s comme le tout premier
   essai) : la requête a réellement fait OOM-killer le pod cette fois
   (exit 137), pas juste timeout côté client — invalide l'hypothèse
   "il suffit de la laisser finir". Mémoire montée 411Mi→3543Mi avant le
   timeout client du premier essai, puis restée sur un plateau élevé
   plusieurs minutes avant de redescendre naturellement (comme lors de
   l'incident du 17/08 précédent) — a nécessité une pause active
   (surveillance jusqu'à repasser sous 1500Mi) avant tout nouvel essai.
2. Script modifié pour éviter complètement `first()` : nouvel argument
   `--jour-min` (miroir de `--jour-max` existant) qui court-circuite la
   découverte par requête en acceptant une date de départ déjà connue.
   Utilisé avec `--jour-min 2025-11-01` (marge de 20 jours avant le
   21/11/2025 documenté plus haut dans ce fichier, section "167 jours
   (357 fichiers) | 21/11/2025 → 27/07/2026") — **a confirmé exactement
   cette date** (20 jours vides puis 178 966 points le 21/11/2025,
   migrés et vérifiés). Mais la requête du jour SUIVANT (22/11/2025) —
   le motif `_points_jour()` (range 1 jour + `group(columns:["_field"])`
   + `pivot()`) pourtant déjà validé sûr sur `mesures_capteurs` (5888
   lots sans incident) et en Phase 0 (3,17s mesuré) — a de nouveau fait
   OOM-killer le pod.

**Les deux fois : récupération automatique confirmée (k3s), aucune perte
de données** (le motif DELETE-avant-INSERT + vérification par comptage a
laissé la transaction en échec proprement annulée — 0 ligne orpheline
pour le 22/11/2025 vérifié directement en base), production non affectée
(retrait `statut: ok`, 8 sources actives, heartbeats à jour les deux
fois).

**Diagnostic affiné** : le fait qu'un motif de requête déjà prouvé sûr
(le `_points_jour()` journalier) ait quand même fait planter le pod sur
`mesures_dewesoft` change la lecture par rapport à l'incident précédent —
ce n'est probablement pas (uniquement) un état interne dégradé par les
redémarrages, mais une caractéristique structurelle de cette mesure
spécifique : échantillonnage continu à 10 Hz sur 8 canaux, un ordre de
grandeur plus dense que tout ce qui a été migré sans incident aujourd'hui
(`mesures_capteurs` : quelques points toutes les heures). Un jour de
`mesures_dewesoft` pour un seul canal peut représenter plusieurs centaines
de milliers de points — largement plus volumineux que n'importe quel lot
des 4 autres mesures.

**Décision (accord explicite de l'utilisateur, "oui")** : `mesures_dewesoft`
mise en pause à nouveau, sans nouvel essai à l'aveugle. Les 4 autres
mesures restent acquises (migrées et vérifiées). Prochaines pistes à
évaluer froidement avant une nouvelle tentative : fenêtres plus petites
qu'un jour (par heure ?) pour `_points_jour()` sur cette mesure
spécifiquement, et/ou augmentation de la limite mémoire du pod InfluxDB
(actuellement 4Gi) si la RAM du nœud le permet.

### RAM InfluxDB relevée à 8Gi, reprise puis panne totale de production (18/08/2026)

**Reprise avec RAM augmentée.** Piste RAM retenue en premier : InfluxDB
relevé 4Gi → 8Gi (nœud à 23Gi RAM, large marge). Retest à froid confirmé
rapide (0,614s). `mesures_capteurs` migré intégralement (59/59 adresses
MAC, 5888 lots, aucun écart) — deux bugs de script trouvés et corrigés au
passage (`pivot()` perdant les tags point-par-point pour les mesures sans
tag de découpage ; lookback silencieux de -30j par défaut sur
`schema.tagValues()`). `mesures_dewesoft` relancé avec la RAM à 8Gi : un
nouvel argument `--jour-min` ajouté pour éviter `first()` (identifié comme
significativement plus coûteux que `last()`, a fait OOM-killer le pod même
avec un timeout client remonté à 120s) — a confirmé exactement la date de
campagne documentée (21/11/2025) et migré ce jour avec succès. Watchdog
ajouté (`setsid`, relance automatique sur crash, même principe que le
watchdog DeweSoft) pour rendre la migration autonome — élargie aux 8
canaux d'un coup plutôt qu'un par un. 136 jours HA1 migrés avant un
troisième OOM (à 8Gi cette fois), le watchdog relançant automatiquement
comme prévu.

**Panne totale de production, découverte ~09h15 le 18/08/2026, cause
réelle : saturation disque.** Le PVC TimescaleDB (nominal 30Gi, mais
`local-path-provisioner` ne fait respecter aucune limite réelle) avait
grossi à **135 Go** pour HA1+HA2 seuls — extrapolé aux 8 canaux, largement
au-delà des 193 Go de disque total du VPS. Le disque saturé à 93% a fait
passer le nœud k3s en condition `DiskPressure` à 02h26, évinçant **tous**
les pods de production (InfluxDB, Kafka, Mosquitto, webapp, Grafana,
TimescaleDB) — panne totale pendant ~7h, jusqu'à intervention manuelle.
**Verrou mort découvert en résolvant** : le pod d'aide de
`local-path-provisioner` chargé de supprimer l'ancien volume se faisait
lui-même évincer faute de disque — contourné en supprimant le répertoire
directement sur le nœud (`sudo rm -rf`, watchdog arrêté d'abord pour ne
pas relancer d'écriture pendant l'opération). Nettoyage sûr (cache
Docker/journal/pip, ~2,7 Go) tenté en premier, insuffisant seul. Une fois
le répertoire supprimé : disque 93%→21%, `DiskPressure` levée après le
délai de transition de kubelet (5 min), tous les pods reprogrammés.
**Effet de bord découvert pendant la remise en route** : `docker system
prune -af` (fait pendant le nettoyage d'urgence) avait supprimé les 3
images locales du projet (`murmetric-webapp`, `murmetric-kafka-consumer`,
`murmetric-bridge`) — jamais poussées vers un registre, invisibles pour
un prune qui ne regarde que les conteneurs actifs. Reconstruites depuis
les Dockerfile du dépôt sur le VPS, pods forcés à se recréer avec les
images fraîches. **Aucune perte de données confirmée** côté pipelines
(buffer SQLite PC Amiens vide après coup, dernier fichier .dxd traité
avant le début de la panne).

### Décision finale : abandon de TimescaleDB, InfluxQL à la place (18/08/2026)

Face à un chantier de migration devenu chronophage et risqué (4 incidents
de production le 17/08, une panne totale de 7h le 18/08, toutes deux
causées par le volume de `mesures_dewesoft`), l'utilisateur a proposé une
alternative après ses propres recherches : garder InfluxDB tel quel, mais
utiliser InfluxQL (dialecte proche du SQL, couche de compatibilité
InfluxDB v1 déjà intégrée à InfluxDB v2) pour les requêtes Grafana plutôt
que Flux. Alternative écartée en même temps : passer à InfluxDB 3.0 (SQL
standard via Apache DataFusion) — analysé et rejeté, car il n'existe pas
de mise à niveau in-place 2.x→3.0 (formats de stockage incompatibles) :
cela aurait réintroduit exactement le risque qu'on cherchait à éviter
(migrer le même gros volume), vers un produit auto-hébergé encore moins
éprouvé que TimescaleDB.

**Implémenté** (`k8s/grafana/configmap.yaml`) : deuxième datasource
Grafana provisionnée à côté de la Flux existante, pointant sur la même
instance InfluxDB (aucune donnée déplacée). **Piège découvert en testant** :
la combinaison `jsonData.version: InfluxQL` + `secureJsonData.token`
(mécanisme d'authentification par défaut, hérité du mode Flux) échoue
silencieusement côté Grafana — l'erreur ne sort même pas jusqu'à InfluxDB
(confirmé : aucune requête reçue côté logs InfluxDB pendant l'échec).
Résolu avec la méthode exacte que l'utilisateur avait trouvée par ses
propres recherches : en-tête HTTP personnalisé explicite
(`httpHeaderName1: Authorization` / `secureJsonData.httpHeaderValue1: Token
${INFLUX_TOKEN}`) plutôt que le champ token dédié — validé de bout en bout
via l'API `/api/ds/query` de Grafana (macros `$timeFilter`/`$__interval`
comprises) avant tout déploiement sur le dashboard réel.

**Les 9 panels du dashboard** (`k8s/grafana/dashboards/hr-t-socma.json`)
réécrits en InfluxQL. **Piège retrouvé, déjà documenté dans ce projet**
(section 32, investigation InfluxDB du 13/08) : combiner plusieurs canaux
dans un seul filtre `OR` reste disproportionnellement coûteux — une
première tentative de consolider les 4 canaux de chaque panel retrait en
une seule requête (`canal_nom = 'HA1' OR ... OR canal_nom = 'VA2'`) a fait
grimper InfluxDB à 700m CPU (plafond) et time-out après 30s côté Grafana.
Revenu à la structure d'origine (4 requêtes séparées, une par canal,
comme les panels Flux existants) — validé à 8,3s pour un seul canal sur
90 jours, sans incident InfluxDB.

**Décommissionnement TimescaleDB** (accord explicite, "on abandonne
timescaledb") : StatefulSet/PVC/Service/ConfigMap supprimés du VPS, clé
`timescaledb-password` retirée du secret `murmetric-secrets` et du
template, `migration_influx_timescale.py` et `k8s/timescaledb/` supprimés
du dépôt. InfluxDB reste l'unique source de données, Flux et InfluxQL
coexistent comme deux façons d'interroger la même base — Flux toujours
utilisé par le backend de la webapp (assistant IA, `mesures.py`, etc.),
hors du périmètre de ce changement.

## 34. Corrections d'intégrité des registres d'édition (18/08/2026)

### Bug trouvé et corrigé — doublon silencieux en corrigeant une saisie teneur en eau

Question utilisateur sur une capture d'écran ("est-ce qu'une modification de
valeur ici sera répercutée en base ?") → investigation du code de
`PUT /api/teneur_eau`. `corriger()` (`teneur_eau.py`) ne supprimait le point
InfluxDB original que si mur/couche/date changeaient — or le formulaire
d'édition n'a pas de champ date, donc en pratique cette condition n'est
presque jamais vraie. Le tagset écrit inclut `utilisateur_id`/`utilisateur_nom`
de la personne qui **édite**, pas de l'auteur d'origine : si un utilisateur
différent corrige une saisie sans changer mur/couche (le cas normal), le
nouveau point ne correspond plus au tagset de l'ancien — InfluxDB écrit un
second point au même timestamp au lieu d'écraser le premier. Doublon masqué
par le regroupement mur/couche/date du frontend (une seule ligne affichée,
deux points réels en base). **Corrigé** : suppression désormais
systématique avant réécriture, peu importe l'identité ou l'auteur — le
prédicat de suppression ne filtre jamais par utilisateur, donc retrouve le
point d'origine quel que soit qui l'avait créé. Déployé et vérifié (rebuild
image webapp).

### Vérifié sans bug — registres Capteurs HR/T et Canaux retrait

Question de suivi : les tableaux "Capteurs HR/T" et "Canaux retrait" (même
motif Éditer/Enregistrer visuellement) ont-ils le même risque ? Investigué :
non — architecture différente. Ces deux tableaux sont un seul et même
mécanisme (`capteurs.py`, `_modifier_entree()`) : un fichier JSON
(`capteurs.json`/`capteurs_retrait.json`) sur le volume persistant,
modification **par clé** (adresse MAC / nom de canal) directement dans le
dict, jamais de tagset ni de notion d'auteur écrite dans la donnée. Verrou
`threading.Lock()` partagé + déploiement mono-processus/mono-réplique
confirmé (`Dockerfile.webapp` sans `--workers`, `replicas: 1`) — aucune
course possible entre requêtes. Seule limite résiduelle (mineure, sans
rapport avec le bug teneur en eau) : deux éditions strictement simultanées
de la **même** ligne par deux personnes différentes suivent un simple
"dernier écrit gagne" par champ, sans version/ETag.

### Réétiquetage rétroactif InfluxDB — Capteurs HR/T uniquement

Question de suivi : éditer mur/couche/etc. dans la webapp propage-t-il vers
InfluxDB ? Oui mais seulement pour les **nouvelles** mesures — les scripts
d'ingestion (Pi/PC Amiens) relisent le registre toutes les
`CAPTEURS_RAFRAICHISSEMENT_S` (60s par défaut) et taguent chaque nouveau
point avec l'étiquetage courant, mais rien ne réécrit rétroactivement
l'historique déjà en base. Impact concret : renommer une couche crée une
discontinuité — l'ancien historique reste sous l'ancien nom, invisible avec
un filtre sur le nouveau.

Deux solutions analysées : réécriture physique de l'historique (simple,
sans dette de maintenance, mais coûteuse/risquée sur un gros volume) vs.
alias au moment de la requête (fonctionne même sur un gros volume, mais
demande de maintenir la logique d'alias à chaque endroit qui filtre par ces
tags — backend ET Grafana — un vrai risque de dette, dans la même veine que
plusieurs bugs Flux "par table" déjà rencontrés cette session).

**Implémenté pour `mesures_capteurs` uniquement** (décision explicite —
`mesures_dewesoft` exclu, volume sans commune mesure et fragilité
InfluxDB démontrée toute la journée du 17-18/08) :
- `_tags_capteur()` (`capteurs.py`) : reconstruit les tags InfluxDB tels que
  `kafka_consumer_influx.py` les écrirait pour un capteur donné (mêmes
  valeurs de repli "Non défini"/"Inconnu"), pour comparer avant/après une
  modification.
- `_reetiqueter_mesures_capteurs()` : si un tag InfluxDB pertinent a changé
  (`emplacement`/`nom_capteur`/`nom_couche`/`nom_mur`/`position`/`rd` —
  `ingestion`/`prestation` ne sont pas des tags InfluxDB pour cette mesure,
  donc sans effet), lit tout l'historique du capteur (borné à
  `2024-01-01`, comme le reste du projet), reconstruit chaque ligne avec
  les nouveaux tags en préservant horodatage et valeurs de champs exactes
  (gestion typée bool/int/float pour respecter le Line Protocol), supprime
  l'ancien historique par prédicat `adresse_mac`, réécrit. Même principe
  delete-by-predicate + réécriture que `teneur_eau.corriger()` — appliqué
  ici sans la faille du bug ci-dessus (le prédicat ne dépend jamais de
  qui édite).
- Câblé dans `PUT /api/capteurs/hr_t/{mac}` uniquement (pas
  `/retrait/{canal}`).

**Vérifié en conditions réelles** (capteur `C2123F8D`, 1507 points sur
l'historique complet) : relabellisation test (`nom_couche` →
"TEST_REETIQUETAGE_TEMPORAIRE") puis retour à la valeur d'origine —
confirmé à chaque étape par requête InfluxDB directe : ancien tag
totalement absent après réétiquetage, valeurs de champs (température,
humidité, point de rosée, `mac_complete_connue`) identiques au bit près au
même horodatage, nombre de points final identique (1507) — aucune perte ni
duplication. Déployé (rebuild image webapp) et testé directement en base,
sans passer par l'UI.

### Canaux retrait : avertissement à l'édition + sélecteur de canal en liste déroulante

Suite de la discussion précédente : pour `mesures_dewesoft`, le
réétiquetage rétroactif est délibérément exclu (volume, fragilité
InfluxDB démontrée le 17-18/08) — la discontinuité mur/couche/position
entre ancien et nouvel historique reste donc possible après une édition.
Analyse de l'impact réel : **aucun effet sur le dashboard Grafana** (les
panels retrait filtrent uniquement par `canal_nom`, jamais par
mur/couche/position, cf. section 33) ; effet réel côté webapp (Vue
d'ensemble, statistiques, nomogramme, assistant IA), qui exposent
mur/couche comme mode de sélection le plus naturel. Découverte
supplémentaire en creusant `/api/mesures/valeurs-tags` : côté retrait,
les listes Mur/Couche sont peuplées depuis le **registre actuel**
(`capteurs_retrait.json`), pas depuis l'historique InfluxDB — après un
renommage, l'ancienne valeur disparaît purement et simplement de la
liste déroulante (pas juste "filtrée à vide"). `canal_nom` (clé du
registre, jamais renommée) reste le seul sélecteur garanti de retrouver
l'historique complet d'un canal quel que soit son étiquetage passé.

**Implémenté** :
- `SelecteurMesure.jsx` : le champ "Canal" (visible seulement pour
  `type === "retrait"`) passe d'un `<input>` texte libre (aucune
  validation, aucune liste — "ex. HA1" en indication seulement) à un
  `<select>` strict peuplé dynamiquement depuis
  `combinaisons.map(c => c.canal_nom)`, même mécanisme et même
  justification déjà documentée pour Mur/Couche (éviter la classe de bug
  `<input list>`/datalist rencontrée deux fois le 14/08/2026).
- `Capteurs.jsx` : nouvelle prop `avertissementEdition` sur
  `TableauCapteurs`, affichée uniquement pendant l'édition d'une ligne —
  câblée sur l'instance "Canaux retrait" seulement (pas "Capteurs HR/T",
  qui réétiquette maintenant rétroactivement et n'a donc plus ce
  problème). Nouvelle classe CSS `.avertissement` (`index.css`, ambre,
  distincte de `.erreur` en rouge).

**Vérifié** : build Vite, `oxlint`, `prettier` tous passants ; endpoint
`/api/mesures/valeurs-tags?type=retrait` confirmé renvoyer les 8
`canal_nom` réels (HA1/HA2/VA1/VA2/HB1/HB2/VB1/VB2) après déploiement.
**Non testé visuellement dans un navigateur** (pas d'outil de test
navigateur disponible dans cet environnement) — vérifié uniquement par
build/lint/déploiement/réponse API réelle, comme les changements
frontend précédents de cette session.

### Panels retrait Grafana lents — CPU InfluxDB relevé + fenêtre par défaut réduite (18/08/2026)

Question utilisateur suite à une capture d'écran (les 4 panels retrait
restaient vides longtemps). **Chronométré en direct** via l'API
`/api/ds/query` de Grafana (mêmes requêtes InfluxQL exactes que les
panels) : un seul canal sur 90 jours = 9,4s ; les 4 canaux d'un panel
sont exécutés **séquentiellement** par le datasource (pas en parallèle)
= 32,9s pour un panel complet. Cause : `mesures_dewesoft` à 10 Hz sur 90
jours représente ~78 millions de points bruts par canal à parcourir
avant agrégation — pas un défaut de requête (la consolidation
multi-canaux en un seul filtre `OR`, plus rapide en théorie, reste
disproportionnellement coûteuse, déjà écarté plus haut dans cette
section).

**Deux correctifs appliqués successivement, chacun revérifié par
chronométrage réel** :
1. CPU InfluxDB `k8s/influxdb/statefulset.yaml` : 700m → 1500m (nœud 4
   cœurs, 67% déjà alloué en limites tous pods confondus — marge
   disponible). Effet mesuré : 32,9s → 22,1s pour le même panel (90j).
2. Fenêtre par défaut des 4 panels retrait (`hr-t-socma.json`, panels
   id 5/6/8/9) : 90 → 30 jours (titres mis à jour en conséquence),
   version dashboard 7→8. Effet mesuré, cumulé avec le point 1 : 22,1s
   → **6,4s** pour un panel complet — soit ~5x plus rapide que le point
   de départ.

**Vérifié après déploiement** : InfluxDB saine tout du long (0
redémarrage, mémoire jamais au-dessus de ~3,7Gi pendant les tests,
largement sous le plafond de 8Gi), dashboard confirmé en version 8 avec
les 4 panels à `timeFrom: 30d` via l'API Grafana.

## 35. Export en masse retrait/HR-T/teneur en eau — CSV/Parquet (18/08/2026)

### Demande et deux incidents de production en cherchant le bon calibrage

Question utilisateur ("possibilité d'exporter toutes les données de
retrait/HR-T/point de rosée en CSV ?") → nouvelle page **Export**
(`/export`, entre "Teneur en eau" et "Capteurs" dans la navigation),
nouveau routeur `export.py`. Première version : lecture via
`query_api().query()` (méthode "simple" de la librairie InfluxDB,
matérialise tout le résultat en objets Python avant de le rendre) +
garde-fou "canal-jours" calibré à la main. **Deux incidents en testant
ce garde-fou contre la production** :
1. Un export "brut" de 1 canal × 2 jours (1,7 million de points) a fait
   OOM-killer mon propre script de test — révèle que la limite mémoire
   du pod webapp (256Mi) était largement insuffisante pour ce genre de
   traitement, jamais anticipée à la conception (dimensionnée pour une
   API JSON classique).
2. Après avoir remonté la mémoire à 1Gi et corrigé le garde-fou pour
   tenir compte du nombre de canaux (pas seulement des jours), **la
   même requête (1 canal × 2 jours, dans les limites du nouveau
   garde-fou) a fait OOM-killer le pod de production réel** — pas juste
   un script de test cette fois. Confirmé récupéré automatiquement
   (k3s), aucune perte de service au-delà du redémarrage du pod.

**Cause racine identifiée** : `query_api().query()` matérialise
l'intégralité du résultat InfluxDB en objets Python (`FluxRecord`, un
par point, chacun portant toutes les colonnes y compris les tags
répétés) avant de le rendre — plusieurs copies successives du même jeu
de données (réponse HTTP brute, objets `FluxRecord`, dictionnaire de
fusion, texte CSV final) coexistent en mémoire au pic. Un simple
plafond "canal-jours", quelle que soit sa valeur, ne pouvait pas
compenser ce problème structurel — deux tentatives de calibrage ont
toutes les deux échoué contre la production.

### Refonte complète — lecture en flux, jour par jour

**Deux principes stricts, appliqués systématiquement** :
1. **`query_api().query_stream()`** (pas `.query()`) : ne matérialise
   jamais plus d'un enregistrement InfluxDB à la fois, quelle que soit
   la taille du résultat — méthode déjà présente dans la même
   librairie, jamais utilisée ailleurs dans ce projet avant.
2. **Requêtes bornées jour par jour** (même règle que le reste du
   projet pour `mesures_dewesoft`), jamais un filtre combinant
   plusieurs canaux — chaque canal a son propre générateur Python,
   fusionnés ensuite par horodatage (`_fusionner_par_temps()`, un vrai
   merge à N voies, tolérant un horodatage absent sur un canal sans
   désynchroniser les autres — pas un `zip()` naïf, qui aurait
   silencieusement décalé les données au moindre trou).

Effet : mémoire utilisée à peu près constante, indépendante de la
période demandée — le garde-fou "canal-jours" devient inutile et a été
**retiré entièrement**, pas juste recalibré une troisième fois.

**Sérialisation** : CSV en flux direct (`StreamingResponse`, un objet
"fichier" minimal qui laisse `csv.writer` échapper correctement tout en
restant un générateur) ; **Parquet ajouté comme format alternatif**
(nouvelle dépendance `pyarrow`) — écrit par lots de 50 000 lignes vers
un fichier temporaire (le format Parquet, avec son pied de fichier écrit
à la fin, ne se prête pas à un envoi HTTP progressif comme le CSV texte),
servi puis supprimé automatiquement après envoi.

**Deux modes de livraison** :
- **Téléchargement direct** : réponse HTTP en flux, adapté à une
  période raisonnable.
- **Tâche de fond** (`POST /retrait/tache` + `GET .../tache/{id}` pour
  le suivi + `GET .../tache/{id}/telecharger`) : génère le fichier
  progressivement sur le volume persistant (`/data/exports/`), suivi
  d'avancement en jours traités, adapté à une période longue ou à tout
  l'historique — même moteur de lecture que le mode direct, juste écrit
  sur disque au lieu d'être streamé au navigateur. Suivi en mémoire
  (pas en base) : une tâche en cours est perdue si le pod redémarre,
  acceptable vu le déploiement mono-réplique. Fichier de tâche **non
  supprimé après téléchargement** (permet un re-téléchargement) —
  ménage occasionnel de `/data/exports/` à prévoir manuellement, pas
  encore automatisé.

CPU du pod webapp 200m → 500m au passage : la lecture en flux (un
enregistrement à la fois, par design) coûte plus de CPU que l'ancienne
lecture en bloc — mesuré au plafond pendant un test, causant un export
lent avant l'augmentation.

**Vérifié en conditions réelles, en escaladant prudemment** (mémoire
observée à chaque étape) :
- 1 canal, 1h, brut : mémoire stable 82-87Mi (contre OOM immédiat
  avant la refonte).
- 3 canaux, 7 jours, agrégé horaire : 1,15s, mémoire inchangée, fusion
  multi-canaux correcte (colonnes HA1/HA2/VA1 alignées).
- **1 canal, 1 jour COMPLET, brut (864 000 points, le cas qui avait
  fait planter la production)** : réussi, 864 001 lignes exactes,
  46,5 Mo, mémoire restée à 84Mi du début à la fin (6m29s — lent, mais
  sûr, InfluxDB restée saine tout du long).
- Tâche de fond de bout en bout (démarrage → suivi → téléchargement) :
  un bug trouvé et corrigé au passage (le dossier `/data/exports/`
  n'était créé que dans le chemin d'export direct, pas dans la tâche de
  fond — `FileNotFoundError` à la première tentative, corrigé en une
  ligne).

### Suite (18/08/2026) — nom de fichier avec période + rien ne persiste sur le VPS

Deux questions utilisateur après les premiers tests ont mené à deux
correctifs supplémentaires, mêmes principes appliqués partout :

1. **Nom de fichier incluant la période demandée.** Avant : noms fixes
   (`retrait_export.csv`, `hr_t_export.csv`, `teneur_eau_export.csv`,
   ou juste `retrait_{champ}_{resolution}.csv` sans dates) — une fois
   téléchargés, impossible de distinguer deux exports sans rouvrir
   chacun. Corrigé pour les quatre chemins (export direct retrait,
   HR/T, teneur en eau, et téléchargement de tâche de fond) : le nom
   inclut désormais `{debut}_{fin}` (dates ISO, ex.
   `retrait_valeur_filtree_heure_2026-06-19_2026-06-19.csv`). Pour la
   tâche de fond, `champ`/`resolution`/`debut`/`fin` sont maintenant
   conservés dans le dictionnaire de suivi (`_taches[tache_id]`) au
   moment du démarrage, pour être disponibles au moment du
   téléchargement (l'identifiant de tâche seul ne les portait pas).

2. **Les fichiers de tâche de fond ne doivent plus persister sur le
   VPS après téléchargement.** Le mode "téléchargement direct" ne
   laissait déjà rien sur le VPS (CSV jamais écrit sur disque ; fichier
   Parquet temporaire déjà supprimé après envoi via
   `BackgroundTask`) — seul le mode "tâche de fond" laissait le fichier
   en place après un premier téléchargement (choix initial délibéré,
   pour permettre un re-téléchargement sans relancer la génération).
   Ce choix est revenu sur demande explicite, en cohérence avec
   l'incident de disque plein de la veille (section 34) : plus rien ne
   doit s'accumuler par défaut sur `/data/exports/`. Le même mécanisme
   `BackgroundTask` (déjà utilisé pour le Parquet temporaire) supprime
   maintenant le fichier de tâche juste après l'envoi réussi au
   navigateur, et fait passer le statut de la tâche de `"termine"` à
   `"telecharge"` — plutôt que de le laisser indéfiniment "terminé"
   alors que le fichier n'existe plus. Contrepartie assumée : un second
   téléchargement nécessite de relancer la tâche (source InfluxDB
   intacte, aucune perte de données, juste à régénérer). Frontend
   (`Export.jsx`) mis à jour pour afficher ce nouveau statut plutôt que
   de ne rien afficher pour une valeur inconnue.

Vérifié en conditions réelles (1 canal, 2h, agrégé horaire, tâche de
fond) : nom de fichier correct pour l'export direct **et** pour le
téléchargement de tâche
(`retrait_valeur_filtree_heure_2026-06-19_2026-06-19.csv`) ; après le
premier téléchargement, `/data/exports/` confirmé vide (`ls`) et
l'état de la tâche renvoie `"statut": "telecharge"` ; une seconde
tentative de téléchargement renvoie désormais `409 Conflict` (bloquée
par le contrôle de statut, plus par un fichier manquant).

## 36. Assistant IA — coller/importer une image arbitraire (18/08/2026)

Question utilisateur : possible de coller une capture d'écran dans la
zone de prompt de l'assistant, pas seulement d'envoyer le graphique déjà
tracé par l'appli ? Réponse : non à ce moment-là, seul le bouton
"Envoyer avec le graphique" existait (capture auto du `<svg>` affiché).
Mais le backend n'avait aucune dépendance structurelle à ce mode —
`POST /api/assistant/chat-image` (`DemandeChatImage`) accepte déjà
n'importe quelle `image_data_uri` avec `selection` optionnelle
(`Selection | None = None`) — seul le frontend imposait ce chemin.
Implémenté côté frontend uniquement (`Assistant.jsx`), aucun changement
backend :

- **Collage** : gestionnaire `onPaste` sur le textarea — détecte une
  image dans `clipboardData.items`, l'intercepte (`preventDefault`)
  pour ne pas polluer le texte, laisse le collage de texte normal
  intact si aucune image n'est présente.
- **Import de fichier** : bouton "Joindre une image" + `<input
  type="file">` caché, alternative au collage (plus découvrable, utile
  hors clipboard).
- Image affichée en aperçu (vignette + bouton "Retirer") avant envoi,
  retirée automatiquement une fois le message envoyé.
- **Limite de taille côté client** : 8 Mo (`TAILLE_MAX_IMAGE`), rejetée
  avec un message clair avant tout envoi réseau plutôt que d'échouer
  côté API après coup.
- **Décision volontaire** : contrairement au bouton "Envoyer avec le
  graphique" (qui joint la `selection` mur/couche courante, cohérente
  avec l'image envoyée), une image collée/importée est envoyée **sans**
  `selection` — l'associer aux statistiques de la sélection affichée
  aurait été trompeur pour une capture étrangère à l'appli (ex. un
  graphique Excel, une photo de terrain).
- Les deux boutons ("Envoyer avec le graphique" et collage/import)
  restent indépendants et non cumulables en un seul envoi — scope
  volontairement limité, pas de système multi-image.

**Vérification** : pas d'outil de test navigateur disponible dans cet
environnement (comme pour les précédentes fonctionnalités UI de ce
projet) — vérifié par build de production (`vite build`, aucune erreur)
+ lint (`prettier`/`oxlint` propres) + confirmation que la chaîne
"Joindre une image" est bien présente dans le bundle déployé sur le
VPS. Le chemin backend exact emprunté par cette fonctionnalité (image
+ prompt, **sans** `selection`) a été testé en conditions réelles
contre l'API Gemini de production, avec une image PNG de test générée
localement (carré rouge uni) : `POST /api/assistant/chat-image` a
répondu `200`, Gemini a correctement identifié "Rouge" comme couleur
dominante — confirme que le chemin sans `selection` fonctionne de bout
en bout. Un premier essai a échoué (`503` puis timeout apparent côté
client) : diagnostiqué comme un vrai `503 "This model is currently
experiencing high demand"` retourné par Gemini lui-même (reproduit par
un appel direct au SDK OpenAI, hors de l'endpoint), pas un bug du
nouveau code — un second essai a immédiatement réussi.

### Suite (18/08/2026) — bouton "Réessayer" sur erreur transitoire

Un `503 "high demand"` Gemini authentique est remonté à l'utilisateur en
usage réel juste après la mise en prod ci-dessus, avec le message "Analyse
d'image échouée — Gemini : erreur API (code 503)." (le SDK OpenAI retente
déjà 2 fois automatiquement en arrière-plan avant d'abandonner — le
message affiché a donc déjà survécu à plusieurs tentatives infructueuses
en quelques secondes). Question complémentaire de l'utilisateur : pourquoi
pas un LLM vision local sur le VPS en repli de Gemini, vu l'usage
ponctuel ? Réponse donnée et actée : déconseillé (pas de GPU sur le VPS,
4 vCPU/23 Go déjà partagés avec d'autres projets — Ollama déjà écarté pour
l'assistant principal pour cette même raison en section 32 —, qualité de
lecture de courbe nettement inférieure sur exactement la tâche visée),
préféré une solution ciblant directement le problème réel (un pic
transitoire, pas une indisponibilité structurelle).

Implémenté à la place : bouton "Réessayer" à côté du message d'erreur,
qui rejoue la requête échouée à l'identique (texte + image déjà
capturée/collée, sans redemander quoi que ce soit à l'utilisateur).
`envoyer()` factorisé en deux fonctions : `executerEnvoi()` (l'appel
réseau proprement dit, réutilisable) et `envoyer()` (capture le
texte/l'image une seule fois, y compris pour "graphique" — la capture
SVG→image n'est donc plus refaite à chaque réessai). En cas d'échec, la
requête complète (`question`, `mode`, `source`, `imageDataUri`) est
gardée en état (`dernierEchec`) jusqu'au prochain envoi réussi ou
nouvelle tentative. Aucun changement backend. Vérifié par build/lint
(pas d'outil de test navigateur disponible, comme pour le reste de cette
fonctionnalité) et confirmation que la chaîne "Réessayer" est bien
présente dans le bundle déployé sur le VPS.

### Suite immédiate (18/08/2026) — message de quota Gemini trompeur, corrigé

En cliquant "Réessayer" à plusieurs reprises, un vrai `429` Gemini
("quota d'appels atteint pour aujourd'hui — réessaie plus tard...")
persiste anormalement longtemps. Diagnostic en direct (appel SDK OpenAI
isolé, hors endpoint, contre la vraie clé Gemini de prod) : le corps
d'erreur complet de Gemini contient DEUX informations contradictoires —
un `google.rpc.RetryInfo` avec `retryDelay: "30s"` (implique un débit
par minute), mais aussi un `google.rpc.QuotaFailure` avec
`quotaId: "GenerateRequestsPerDayPerProjectPerModel-FreeTier"` (un
plafond JOURNALIER, limite 20 requêtes/jour pour `gemini-3.7-flash` sur
le plan gratuit — confirmé en attendant activement plusieurs minutes
avec un polling toutes les 10s : le `429` persiste malgré le
`retryDelay` de quelques secondes déjà écoulé plusieurs fois, prouvant
que c'est bien le quota journalier qui est le facteur bloquant réel, pas
le débit par minute). Le cumul des tests de cette session (miens +
utilisateur) a très probablement épuisé la totalité des 20 requêtes du
jour à lui seul — limite gratuite extrêmement basse.

Corrigé : `_quota_journalier_epuise()` (nouveau, `assistant.py`) parse
le corps JSON de l'erreur Gemini, cherche `"PerDay"` dans
`quotaId` — si trouvé, message dédié précisant explicitement "ne se
réinitialisera pas avant plusieurs heures, pas dans quelques secondes"
plutôt que le `retryDelay` trompeur de Gemini. Logique de parsing
vérifiée localement contre le payload JSON réel capturé pendant le
diagnostic (`detecte PerDay: True`). Vérifié en conditions réelles
contre `POST /api/assistant/chat-image` de production (quota
effectivement épuisé au moment du test) : message exact reçu —
`"Analyse d'image échouée — Gemini : quota JOURNALIER atteint (plan
gratuit, limite très basse) — ne se réinitialisera pas avant plusieurs
heures, pas dans quelques secondes. Passe sur un plan payant si l'usage
doit être plus soutenu."` Seule vraie solution immédiate : passer la
clé Gemini sur un plan payant (action utilisateur, hors accès Claude
Code).

### Suite (18/08/2026) — modèle Groq texte mort + ajout d'un repli vision

En cliquant "Réessayer" pour une question texte (pas vision), un
`404 model_not_found` Groq est apparu : **`llama-3.3-70b-versatile` a été
totalement décommissionné côté Groq** — absent de
`client.models.list()` pour cette clé, confirmé par appel direct. Le
repli texte censé protéger contre une panne Gemini échouait donc lui
aussi systématiquement, laissant l'assistant totalement inutilisable
dès que Gemini est indisponible (ce qui venait justement d'arriver avec
le quota journalier ci-dessus).

**Remplacement du modèle texte** : liste des modèles actuellement
disponibles sur le compte récupérée en direct, 4 candidats testés avec
le vrai schéma d'outils du projet (`_TOOLS`, appel réel du tool
`interroger_statistiques_mesures` + exécution + réponse finale) :
- `openai/gpt-oss-120b` — fonctionne correctement, retenu.
- `openai/gpt-oss-20b` — échoue à formater ses arguments d'appel d'outil
  en JSON valide.
- `groq/compound` — ne supporte pas les appels d'outils du tout
  (erreur API explicite).
- `qwen/qwen3.6-27b` — n'appelle jamais l'outil, épuise son budget de
  tokens en raisonnement caché avant de produire quoi que ce soit.
`GROQ_MODEL` mis à jour (`config.py` + `k8s/webapp/deployment.yaml`) :
`llama-3.3-70b-versatile` → `openai/gpt-oss-120b`. Validé en conditions
réelles par une boucle complète tool-use (question réelle → appel outil
→ vraies statistiques InfluxDB → réponse finale cohérente).

**Ajout d'un repli vision** (question utilisateur : un des modèles
disponibles a-t-il la vision, sans impacter le VPS ? — remet en
perspective le refus argumenté plus tôt d'un LLM vision **local**,
cf. section 36 plus haut ; un modèle vision **cloud** via une clé déjà
configurée ne pose aucun de ces problèmes, contrairement à un modèle
tournant sur le nœud partagé). Sur les 4 candidats testés en vision,
seul **`qwen/qwen3.6-27b`** accepte un contenu image (les 3 autres
rejettent explicitement : "content must be a string") — confirmé en
identifiant correctement la couleur d'une image de test.

`chat_image()` restructuré sur le même schéma que `chat()` (Gemini
d'abord, repli Groq si échec, erreurs combinées si les deux échouent) —
jusque-là Gemini était le seul fournisseur possible pour ce endpoint,
sans aucun filet. **Piège rencontré et corrigé avant déploiement
définitif** : `qwen/qwen3.6-27b` est un modèle "raisonneur" (bloc de
réflexion interne avant la réponse) — un premier essai avec seulement
`reasoning_format: "hidden"` (masque le raisonnement dans la réponse
mais ne le limite pas) a renvoyé une **réponse vide** en usage réel
malgré `max_tokens=4000`, le raisonnement ayant intégralement consommé
le budget sans qu'aucune réponse finale ne soit produite. Diagnostiqué
en isolant l'appel direct (reproductible mais pas systématique — forte
variance du volume de raisonnement d'un appel à l'autre). Corrigé en
utilisant `reasoning_effort: "none"` (désactive entièrement le
raisonnement plutôt que de le masquer) : réponse correcte en 33 tokens
au lieu d'un budget de plusieurs milliers, `max_tokens` ramené à 2000
(aligné sur Gemini). Garde-fou ajouté au passage : une réponse vide
malgré un statut HTTP 200 est désormais traitée comme un échec
(`RuntimeError`, remonté comme une erreur normale plutôt que renvoyée
telle quelle à l'utilisateur).

Vérifié en conditions réelles contre `POST /api/assistant/chat-image`
de production (Gemini vision toujours en quota journalier épuisé au
moment du test, donc le repli Groq était réellement exercé, pas
seulement disponible en théorie) : deux couleurs de test différentes
(jaune, vert), les deux identifiées correctement,
`"fournisseur": "groq"` confirmé dans la réponse, aucune réponse vide
sur l'ensemble des essais après le correctif `reasoning_effort`.

Bilan : toujours 2 fournisseurs cloud (Gemini + Groq, même clés qu'avant,
aucune nouvelle clé/secret), mais 3 configurations de modèle au total
(texte Gemini, texte Groq repli, vision Groq repli) — zéro impact sur
les ressources du VPS partagé (appels API sortants uniquement, comme
c'était déjà le cas pour Gemini).

## 37. Assistant IA dans Grafana — grafana-llm-app abandonné, Grafana Assistant activé (18-19/08/2026)

Question utilisateur : Grafana propose-t-il un assistant IA intégré, et
comment l'activer sur le Grafana du projet (self-hosted,
`grafana-oss:11.1.0`) ? Deux fonctionnalités distinctes existent côté
Grafana, confirmées via la documentation officielle :
- **Grafana Assistant** (vrai panneau de chat) : nécessite une connexion
  à un compte **Grafana Cloud** même en self-hosted (l'inférence tourne
  côté cloud, le plugin local relaie juste) — écarté d'emblée par
  l'utilisateur pour rester cohérent avec l'architecture 100% VPS du
  projet (aucune dépendance externe non maîtrisée).
- **Grafana LLM plugin** (`grafana-llm-app`) : fonctionnalités IA
  ponctuelles dans certains panels, self-hosted, connecté à un
  fournisseur OpenAI-compatible au choix — choisi comme seule option
  cohérente avec les principes du projet, avec la clé Groq déjà
  configurée (pas Gemini, plafonné à 20 requêtes/jour, cf. section 36).

**Installation réussie** : `GF_INSTALL_PLUGINS=grafana-llm-app` (installe
automatiquement au démarrage, survit à une recréation du PVC — pas
d'installation manuelle à refaire) + provisioning YAML
(`/etc/grafana/provisioning/plugins/`, même mécanisme que les
datasources) pointant vers `https://api.groq.com/openai` avec la clé
Groq (`${GROQ_API_KEY}`, substitution Grafana comme `${INFLUX_TOKEN}`
déjà utilisé pour la datasource InfluxDB). Plugin détecté, enregistré,
config acceptée par l'API — confirmé via `GET
/api/plugins/grafana-llm-app/settings`.

**Bug réel trouvé dans le plugin (v1.0.8, seule version publiée au
catalogue Grafana à cette date)** : configurer un nom de modèle
personnalisé (`jsonData.models`) — indispensable, les modèles Groq ne
s'appellent pas "gpt-4.1"/"gpt-4.1-mini" comme les défauts du plugin —
fait planter systématiquement son health-check (`panic: assignment to
entry in nil map`, `llm_provider.go:71`, `Model.toProvider()` écrit
dans une map jamais initialisée). **Confirmé non lié à notre
configuration** : reproduit à l'identique avec 3 approches différentes
(YAML de provisioning en format plat `models: {base: "...", large:
"..."}`, YAML en format imbriqué `models: {base: {mapping: {openai:
"..."}}}`, et configuration directe via `POST
/api/plugins/grafana-llm-app/settings`) — même stack trace à chaque
fois, avant même le moindre appel réseau vers Groq. Confirmé aussi
qu'aucune version plus récente n'est disponible pour corriger ce bug
(une seule version au catalogue). Sans ce champ, le plugin reste stable
(pas de crash) mais cherche des modèles OpenAI officiels inexistants
sur Groq (`404 model_not_found`) — donc **aucune fonctionnalité IA
utilisable dans les deux cas**, avec ou sans mapping de modèle.

**Piège secondaire trouvé et documenté au passage** (indépendant du bug
principal) : l'URL du fournisseur ne doit **pas** inclure le suffixe
`/v1` (`https://api.groq.com/openai`, pas
`https://api.groq.com/openai/v1`) — le client interne du plugin l'ajoute
déjà lui-même ; avec le suffixe inclus, les requêtes échouaient avec un
chemin dupliqué (`/openai/v1/v1/chat/completions`).

**Décision (accord explicite, "Désinstaller (Recommandé)")** :
entièrement désinstallé et annulé — `grafana cli plugins uninstall`,
retour de `k8s/grafana/deployment.yaml` à son état d'origine (`git
checkout`), suppression du ConfigMap de provisioning
(`grafana-llm-provisioning`) et du fichier local associé, jamais
commité. Grafana revérifié en bonne santé après le retour arrière
(`GET /api/health` → `database: ok`) et confirmé sans aucun plugin
installé (`grafana cli plugins ls` → "no installed plugins found").
Rien à committer sur ce chantier (aucune trace laissée dans le dépôt).

**Pour une reprise future** : ne pas retenter tel quel — le bug est
dans le plugin lui-même, pas dans notre configuration. Vérifier d'abord
si une version plus récente de `grafana-llm-app` a corrigé
`Model.toProvider()` (chercher un changelog mentionnant un fix sur le
mapping de modèles) avant de reprendre cette piste.

### Suite (19/08/2026) — Grafana Assistant activé, avec montée de version

L'utilisateur a créé un compte Grafana Cloud et souhaite reconsidérer
"Grafana Assistant" (écarté initialement pour rester 100% self-hosted).
Clarification apportée : ce n'est pas "tout Grafana Cloud" contre "tout
self-hosted" — Grafana Assistant en mode **self-managed** est un
hybride où seule la fonctionnalité Assistant traverse vers le cloud
(prompts + contexte de requête minimal, jamais les données brutes des
datasources), tout le reste (dashboards, datasource InfluxDB, embedding
dans la webapp) reste sur le VPS sans changement. Retenu par
l'utilisateur.

Plugin `grafana-assistant-app` (v2.0.52) installé
(`GF_INSTALL_PLUGINS`, même mécanisme que `grafana-llm-app`) — mais
page du plugin affichant *"This plugin doesn't support your version of
Grafana"* : dépendance **Grafana ≥13.0.0**, le projet tournait en
`11.1.0`. Contrairement au chantier `grafana-llm-app`, ce blocage est
un vrai prérequis de version documenté, pas un bug — a nécessité une
montée de version Grafana, un changement plus large que l'installation
d'un plugin.

**Recherche préalable** (avant tout changement) : **Grafana v13.0.0 a
été retiré par Grafana Labs** suite à un bug de migration ayant fait
perdre/revenir en arrière des dashboards et dossiers chez certains
utilisateurs — cible fixée à `13.0.2` (dernier tag stable disponible),
jamais `13.0.0`. Chemin recommandé officiellement : montée séquentielle
11.x → 12.x → 13.x (pas d'interdiction explicite du saut direct, mais
pratique conseillée). Breaking changes pertinents identifiés :
validation stricte du format d'UID des datasources (v12, non-problème :
UID auto-générés déjà conformes), migration de la table d'annotations
nécessitant 2-3x l'espace actuel (v12, non-problème : 58 Mo de données
Grafana contre 147 Go libres sur le VPS), retrait complet de
`grafana-cli`/`grafana-server` au profit de `grafana cli`/`grafana
server` (v13, déjà anticipé), migration automatique des dashboards/
dossiers vers un "unified storage" au démarrage (v13, zone d'incertitude
réelle pour des dashboards provisionnés par fichier — pas documentée
précisément, seul un test réel pouvait trancher).

**Validation sur instance isolée avant toute action en production**
(vu l'incident de disque plein de cette même session sur un autre
chantier, et le bug v13.0.0 confirmé ci-dessus) : pod Grafana temporaire
distinct (`grafana-test-upgrade`), volume copié depuis les vraies
données de production (58 Mo), image `grafana-oss:13.0.2`, mêmes
ConfigMaps de provisioning (datasources/dashboards, montage en lecture
seule, sans risque). Vérifié : migrations de démarrage sans erreur liée
à notre configuration (une seule erreur sans rapport, plugin
Elasticsearch groupé inutilisé) ; les deux datasources InfluxDB (Flux
et InfluxQL) fonctionnelles avec de **vraies requêtes retournant de
vraies données** (heartbeat pipeline retrait) ; dashboard "Vue
d'ensemble SOCMA 1 & 2" provisionné intact ; affichage en iframe (mode
kiosk, celui utilisé par la webapp) toujours opérationnel, aucun header
de blocage ; plugin Grafana Assistant pleinement compatible une fois la
version satisfaite (composant "Connect to Grafana Cloud" exposé,
message d'incompatibilité disparu). Instance de test entièrement
supprimée après validation (pod + volume), production jamais touchée
pendant cette phase.

**Bascule en production**, avec l'accord explicite de l'utilisateur :
sauvegarde du volume Grafana réel prise juste avant
(`grafana_backup_avant_13_20260819.tar.gz`, ~17 Mo, sur le VPS hors du
pod) ; `k8s/grafana/deployment.yaml` mis à jour (image
`grafana-oss:13.0.2`) ; déployé et re-vérifié avec les mêmes contrôles
que sur l'instance de test, cette fois contre les vraies données de
production : migrations réelles sans aucune erreur/avertissement lié à
notre configuration, datasources et dashboard intacts, requête Flux
réelle réussie, embedding iframe opérationnel, plugin Grafana Assistant
confirmé compatible (`version: 13.0.2` via `/api/health`).

**Dernière étape, volontairement non automatisée** : la connexion OAuth
du plugin à Grafana Cloud (*Administration → Plugins → Grafana
Assistant → Connection → Start Connection*) ne peut être faite que par
l'utilisateur lui-même — proposition de le faire à sa place avec ses
identifiants explicitement refusée (pas d'outil de navigateur
disponible dans cet environnement pour un flux OAuth interactif, et
principe général de ne jamais manipuler les identifiants d'un compte
externe même si techniquement possible, cohérent avec la pratique de ce
projet de ne stocker aucun mot de passe en mémoire).

Connexion OAuth finalisée par l'utilisateur, vérifiée active côté
plugin (`isAccessTokenSet: true`, `instanceId` renseigné, `backendUrl`
pointant vers `assistant-prod-eu-west-2.grafana.net` — traitement UE).

### Suite (19/08/2026) — InfluxQL comme datasource par défaut

Question utilisateur : mettre `InfluxDB - MurMetric (InfluxQL)` par
défaut plutôt que la datasource Flux ? Vérifié avant tout changement :
le dashboard de production (`hr-t-socma.json`) référence déjà son
datasource **explicitement par UID** sur chaque panel
(`P15E06DA4BAFBC791`, l'InfluxQL) — donc changer le réglage "par
défaut" n'a **aucun impact** sur l'existant (dashboard et webapp, cette
dernière interrogeant InfluxDB directement en Python, indépendamment de
Grafana). Seul effet réel : le datasource pré-sélectionné dans Explore,
un nouveau panel sans choix explicite, ou l'Assistant sans ciblage
`@nom-datasource` dans le prompt. Changé (`grafana-datasources`
ConfigMap, `isDefault` déplacé de Flux vers InfluxQL) pour cohérence
avec la pratique réelle du projet (Flux abandonné au profit d'InfluxQL
depuis l'abandon de la migration TimescaleDB, section 33 addendum).
Déployé et revérifié : `isDefault: true` confirmé sur InfluxQL via
l'API, dashboard de production toujours opérationnel après coup.

### Suite (19/08/2026) — Assistant muet en usage réel, deux incidents trouvés et corrigés, HTTPS mis en place

Premier vrai test utilisateur de l'Assistant (question triviale "bonjour") :
**rien ne se passe**, aucune réponse, aucune erreur visible. Diagnostic en
plusieurs étapes, chacune ayant révélé un problème réel distinct — aucune
piste n'était un faux problème :

**1. Cause racine identifiée via la console navigateur (F12)** :
`Uncaught TypeError: crypto.randomUUID is not a function`. L'API Web
Crypto `randomUUID()`, utilisée par le plugin pour générer les
identifiants de message, n'est disponible que dans un **contexte
sécurisé** (HTTPS ou `localhost`) — or Grafana était servi en HTTP simple
(`http://89.168.34.201:3000`). L'envoi de message échouait silencieusement
dès le départ, cohérent avec les logs serveur (connexion SSE établie mais
jamais de contenu échangé, coupée après 5 minutes).

**2. Contournement de diagnostic — tunnel SSH vers `localhost`** :
`ssh -L 3000:localhost:3000 ...` pour que le navigateur traite Grafana
comme un contexte sécurisé sans toucher au déploiement. Deux obstacles
annexes rencontrés et résolus en cours de route :
- Clé privée SSH refusée par OpenSSH côté Windows (permissions NTFS trop
  ouvertes, `AUTORITE NT\Utilisateurs authentifiés` et
  `BUILTIN\Utilisateurs` avaient un accès direct) — corrigé avec `icacls
  /remove`, en gardant uniquement l'utilisateur, Administrateurs et
  Système.
- Une fois le tunnel actif, connexion admin refusée avec le mot de passe
  attendu (`admin` + valeur du secret `grafana-admin-password`) — piste
  suivie jusqu'à découvrir l'incident ci-dessous.

**3. Deuxième incident, indépendant : Grafana OOMKilled en production**
(`kubectl describe pod` → `Last State: Terminated, Reason: OOMKilled,
Exit Code: 137`), confirmé au moment précis du test : mémoire mesurée à
**255/256Mi**, littéralement collée à la limite. La 13.0.2 embarque
nettement plus que la 11.1.0 (plugins Drilldown groupés, Grafana
Assistant, couche API unifiée) — 256Mi, dimensionné pour l'ancienne
version, n'était plus suffisant. C'est ce crash qui expliquait le refus
de connexion admin (état incohérent après coupure brutale). Corrigé :
mémoire 256Mi → 768Mi (`k8s/grafana/deployment.yaml`), requêtes 128Mi →
192Mi — marge large, nœud partagé toujours à seulement 47% de mémoire
allouée en limites. Redéployé, revérifié : connexion admin réussie,
mémoire stabilisée à 359/768Mi (47%, sain).

Une fois authentifié via le tunnel, l'Assistant a répondu correctement au
message de test — confirmant le diagnostic n°1 comme cause unique du
silence initial, les deux autres incidents ayant simplement compliqué le
diagnostic en cours de route sans être la cause première.

**4. Passage en HTTPS pour supprimer le besoin du tunnel** (accord
explicite, "go avec sslip.io") : nouveau composant **Caddy**
(`k8s/caddy/`, reverse proxy + obtention/renouvellement automatique de
certificats Let's Encrypt), domaines gratuits `sslip.io` (résolvent
automatiquement vers l'IP du VPS, aucune inscription requise, remplaçable
par un vrai nom de domaine plus tard sans autre changement que la
ConfigMap) :
- `https://grafana.89-168-34-201.sslip.io` → service `grafana:3000`
- `https://webapp.89-168-34-201.sslip.io` → service `murmetric-webapp:8090`

Prérequis vérifiés avant de commencer : ports 80/443 injoignables depuis
l'extérieur au départ (testé en direct, pas depuis le VPS) — Security
List Oracle Cloud à ouvrir manuellement par l'utilisateur (comme pour le
port 3000 en son temps, jamais fait pour 80/443 jusqu'ici). Une fois fait,
confirmé joignables.

**Obstacle trouvé et résolu au déploiement** : Caddy restait
indéfiniment `<pending>` côté IP externe, et Let's Encrypt échouait ses
défis de validation (TLS-ALPN-01 : `tls: unrecognized name` ; HTTP-01 :
`404`). Cause identifiée : **k3s installe Traefik par défaut**, déjà
propriétaire des ports 80/443 sur l'IP publique via son propre
LoadBalancer — jamais utilisé par ce projet (`kubectl get ingress -A` :
aucune ressource, confirmé avant toute action), mais bloquant l'attribution
des mêmes ports à Caddy. Résolu en repassant le Service `traefik` (namespace
`kube-system`) de `LoadBalancer` à `ClusterIP` — libère les ports sans
désinstaller Traefik (réversible, rien retiré du cluster). Caddy a alors
immédiatement obtenu ses deux certificats Let's Encrypt (vérifiés :
émetteur réel, dates de validité correctes, testés depuis l'extérieur en
HTTPS sur les deux domaines).

`murmetric_webapp/frontend/src/pages/Grafana.jsx` mis à jour
(`GRAFANA_BASE` → `https://grafana.89-168-34-201.sslip.io`) — seule
référence en dur à l'ancienne URL HTTP dans le code. Les accès directs
existants (`89.168.34.201:3000`/`:8090`) restent fonctionnels en
parallèle, en HTTP, rien retiré.

**Vérifié en conditions réelles, de bout en bout, sans tunnel** :
utilisateur connecté directement via `https://grafana.89-168-34-201.sslip.io`,
question réelle posée à l'Assistant sur la datasource InfluxQL — réponse
correcte et détaillée reçue (type, URL, langage de requête, 7
measurements détectées, datasource Flux liée identifiée).

**HTTPS passé en porte d'entrée unique** (demande explicite, "https en
principal") : les Services `grafana` et `murmetric-webapp` repassés de
`LoadBalancer` à `ClusterIP` (`k8s/grafana/service.yaml`,
`k8s/webapp/service.yaml`) — plus d'exposition directe des ports
3000/8090 sur l'IP publique, seul Caddy (80/443, HTTPS) reste joignable
depuis l'extérieur. Caddy route en interne vers ces Services par leur nom
DNS de cluster, donc le changement ne l'affecte pas. Choix délibéré de ne
pas ajouter de redirection HTTP → HTTPS sur les anciens ports (ni bookmark
externe à préserver, ni intérêt à maintenir un point d'entrée
supplémentaire) — un accès à l'ancienne adresse échoue simplement
(connexion refusée) plutôt que de rediriger. Vérifié : ports 3000/8090
injoignables depuis l'extérieur, les deux domaines HTTPS toujours
pleinement fonctionnels après coup.

## 38. Retrait de Parquet en export direct + préservation d'état entre onglets (19/08/2026)

Deux demandes distinctes suite à des questions utilisateur sur l'export en
masse et l'ergonomie de l'appli :

**1. Parquet retiré du téléchargement direct** (retrait) : questionnement
sur la prudence de stocker même temporairement un fichier Parquet sur le
volume du VPS en mode direct (`_reponse_fichier`, fichier temporaire —
nécessaire car le format Parquet écrit son pied de fichier à la fin,
incompatible avec un flux HTTP réellement progressif comme le CSV).
Alternative "tmpfs en mémoire" écartée après analyse : sur Kubernetes, un
volume `emptyDir` en mémoire consomme le même budget RAM du pod que celui
justement sécurisé lors de la refonte de section 34/35 — n'aurait fait que
déplacer le risque du disque vers la mémoire, pas le résoudre. Solution
retenue : Parquet disponible **uniquement** en tâche de fond (`/retrait
/tache`), où l'écriture progressive et bornée sur le volume persistant est
déjà surveillée et n'a jamais été signalée comme un risque, contrairement
au direct qui n'a ni suivi ni bornage de taille. `exporter_retrait()`
renvoie désormais une erreur 400 explicite si `format=parquet` est demandé
en direct. CSV direct inchangé (déjà 100 % flux, jamais de fichier sur le
VPS). Scope volontairement limité au retrait — HR/T et teneur en eau
restent "volume négligeable", pas concernés par le même risque
(historique complet + haute fréquence + multi-canaux propres au retrait).
Frontend (`Export.jsx`) : `SelecteurFormat` n'affiche Parquet qu'en mode
"Tâche de fond", et repasse automatiquement en CSV si l'utilisateur revient
sur "Téléchargement direct" avec Parquet déjà sélectionné.

**2. Préservation d'état ciblée entre onglets** (Assistant IA + Vue
d'ensemble) : par défaut, React Router démonte entièrement une page en
changeant d'onglet — toute conversation Assistant IA ou courbe chargée
dans Vue d'ensemble était perdue en revenant dessus. Option "tout garder
monté en permanence" écartée (impact réel : les pages qui interrogent
régulièrement le serveur continueraient de le faire même invisibles,
charge supplémentaire sur un InfluxDB déjà fragile cette session) au
profit d'un état déplacé au-dessus des routes, ciblé sur les deux pages
concernées seulement. Nouveau `EtatPagesContext.jsx` : un contexte React
porté dans `App.jsx`, au-dessus des `<Routes>` — les pages elles-mêmes
continuent de se démonter/remonter normalement à chaque navigation
(comportement React Router inchangé), seul l'état qui les intéresse (pas
les indicateurs de chargement/erreur, purement transitoires) survit,
puisqu'il vit dans un composant qui, lui, ne se démonte jamais tant que
la session reste active. `Assistant.jsx`/`VueEnsemble.jsx` adaptés pour
lire/écrire cet état via `useEtatAssistant()`/`useEtatVueEnsemble()` au
lieu de `useState` local — même noms de variables, changement mécanique,
pas de logique métier modifiée.

Ce que ça préserve : navigation entre onglets, mise en arrière-plan de
l'onglet navigateur, aussi longtemps que voulu. Ce qui réinitialise :
rechargement de page, fermeture de l'onglet/navigateur, déconnexion —
état purement en mémoire côté navigateur, rien de partagé entre onglets/
appareils, rien persisté côté serveur.

Vérifié : build de production réussi, lint propre, endpoint retrait
direct confirmé rejeter `format=parquet` (400, message explicite) tout en
laissant passer CSV direct et Parquet en tâche de fond (aucune
régression). **Comportement interactif du changement d'onglet non testé
en navigateur** (pas d'outil de test navigateur disponible dans cet
environnement, comme pour le reste des fonctionnalités UI de ce projet) —
à valider par l'utilisateur.

## 39. Audit projet + test de portée BLE du Pi — deux correctifs trouvés (19/08/2026)

**Audit général demandé** ("passe de l'ensemble du projet") : infrastructure
saine (tous pods `Running`, mémoire Grafana 383Mi/768Mi et webapp
216Mi/1Gi après les correctifs du jour, disque 25% utilisé), git propre.
Deux résidus mineurs trouvés et supprimés sur accord explicite : un pod
`mosquitto` fantôme (`0/1 Completed`, 11 jours, rattaché au ReplicaSet
actif mais jamais nettoyé par Kubernetes) et un fichier Parquet de test
(818 octets, tâche de fond jamais téléchargée).

**Analyse "traces d'origine IA"** (question utilisateur) : le design
visuel de la webapp lui-même (polices système, pas de hero en dégradé,
thème sombre cohérent avec Grafana, aucun emoji décoratif) ne présente
pas les marqueurs habituels des interfaces générées par IA — jugé peu
susceptible d'être repéré par un tiers sur ce seul critère. Signaux plus
caractéristiques identifiés côté dépôt : structure très uniforme des 84
commits (préfixes conventionnels, corps expliquant le "pourquoi"),
densité et style des commentaires de code référençant systématiquement
des dates et `logique_projet.md`, et l'existence même de ce journal de
décisions chronologique. Recommandation donnée : ne pas modifier le
design (pas le signal réellement révélateur) ; prudence recommandée
avant de vouloir dissimuler le processus de développement, notamment si
le projet a un cadre académique (déclarer l'usage d'outils IA est
généralement attendu plutôt qu'à cacher).

**Test de portée BLE du Pi** (demande utilisateur, "vérifier la
performance/portée du Raspberry+antenne Bluetooth") : deux problèmes
réels trouvés et corrigés en cours de diagnostic, avec accord explicite
avant chaque action.

1. **Antenne USB externe (hci1) trouvée DOWN** — le service tournait
   depuis le début en repli sur l'adaptateur intégré (hci0), plus
   faible, sans qu'aucune alerte ne le signale (le repli est un
   comportement voulu du script, cf. commentaire "hci1 = antenne USB
   externe, repli automatique sur hci0"). Cause : interface **bloquée
   par RF-kill** (soft block, confirmé via `rfkill list` — pas une
   panne matérielle, le dongle USB Realtek était bien détecté par le
   noyau). Corrigé : `rfkill unblock` + `hciconfig hci1 up`, service
   redémarré pour repasser sur hci1. **Gain mesuré, même capteur, avant/
   après** : RSSI passé de **-97 dBm (hci0, quasi injoignable) à
   -58 dBm (hci1)** — un gain d'environ 39 dB. Trois autres capteurs
   BLE désormais visibles avec des RSSI tout aussi bons (-56 à
   -62 dBm), contre un seul détecté (à peine) avant correctif. Répond à
   la question initiale : la portée de l'antenne, une fois réellement
   active, est bonne depuis la position actuelle du Pi.
2. **Régression trouvée, causée par le chantier HTTPS du jour** :
   `CAPTEURS_API_URL` sur le Pi pointait encore vers l'ancienne adresse
   HTTP directe (`http://89.168.34.201:8090`), rendue injoignable par
   le passage des Services Grafana/webapp en `ClusterIP` (section 37
   addendum, plus tôt aujourd'hui) — confirmé dans les logs
   (`journalctl`) : erreur de connexion répétée toutes les ~30-60s
   depuis ce matin. Le Pi restait fonctionnel entre-temps (repli sur le
   cache local `capteurs.json`), mais aucune mise à jour du registre
   (mur/couche/position) ne lui parvenait plus. Corrigé : URL mise à
   jour vers `https://webapp.89-168-34-201.sslip.io` directement dans
   `/home/murmetric/murmetric_pi5/lancer_ingestion_capteurs.sh` sur le
   Pi (fichier non versionné dans ce dépôt — déployé par SFTP à
   l'origine, pas de clone git sur cette machine, cf. section 30/31).
   Vérifié après redémarrage du service : plus aucune erreur de
   connexion, registre capteurs republié sur MQTT (65 capteurs), broker
   MQTT cloud toujours connecté normalement.

Accès Pi via SSH par mot de passe (paramiko, comme pour PC Amiens
historiquement) — mot de passe fourni en session, jamais stocké.

## 40. Télémétrie capteurs HR/T — dernière détection, RSSI, batterie (19/08/2026)

**Origine** : en affichant le tableau "Capteurs HR/T (65)", question
utilisateur sur la pertinence d'exposer le niveau de batterie de chaque
capteur — les capteurs sont noyés dans les parois de test, impossible de
vérifier physiquement une batterie faible avant que le signal ne se
perde. Étendu à la dernière détection (le capteur répond-il encore ?) et
au RSSI (la qualité du signal se dégrade-t-elle avant la perte totale ?).

**Découverte bloquante en cours de conception** : `mesures_capteurs`
(mesures physiques HR/T dans InfluxDB) n'a reçu aucune donnée depuis
40 jours, et 0 des 65 capteurs enregistrés n'ont `ingestion: true`.
Remonté immédiatement à l'utilisateur avant de poursuivre — confirmé
comme **volontaire** ("l'ingestion des capteurs HR/T n'est pas encore
en service"), pas un bug. Conséquence directe sur la conception : la
télémétrie devait être **indépendante du flag `ingestion`**, sans quoi
la fonctionnalité n'aurait rien affiché pour aucun capteur dans l'état
actuel de la campagne — c'est justement son intérêt (surveiller un
capteur avant même son activation).

**Conception** :
- Nouveaux champs dans `capteurs.json` (registre, par capteur HR/T) :
  `derniere_detection` (horodatage ISO 8601 UTC), `dernier_rssi` (dBm),
  `derniere_batterie` (%, uniquement Blue Maestro — le protocole ELA
  n'expose qu'un indicateur binaire faible/normal sous 15%, non décodé
  actuellement par `ingestion_capteurs_bluetooth.py`).
- Nouvel endpoint backend `POST /api/capteurs/hr_t/{mac}/telemetrie`
  (clé d'ingestion `X-Ingestion-Key`, comme les autres routes
  d'ingestion), appelé par le Pi à chaque détection. N'écrit jamais dans
  InfluxDB (pas une mesure physique) et ne déclenche aucun réétiquetage
  — à distinguer de `modifier_capteur_hr_t` (édition humaine).
- Côté Pi (`callback()` dans `ingestion_capteurs_bluetooth.py`) : appel
  `envoyer_telemetrie(mac, rssi, batterie)` placé **avant** le filtre
  `ingestion`, juste après l'auto-enregistrement — throttlé à un envoi
  par capteur toutes les 5 minutes (`TELEMETRIE_INTERVALLE_S`, dict
  `dernier_envoi_telemetrie` en mémoire), pour ne pas solliciter l'API à
  chaque paquet BLE (plusieurs par minute). Best-effort : une erreur
  réseau n'interrompt jamais le scan, retentée au prochain paquet.
- Frontend (`Capteurs.jsx`, tableau "Capteurs HR/T" uniquement — pas
  "Canaux retrait", qui sont des voies DeweSoft filaires sans notion de
  pile/RSSI) : 3 colonnes ajoutées avec pastilles de couleur. Dernière
  détection : vert < 15 min, orange < 3 h, rouge au-delà, gris "Jamais
  vu" si absent — seuils calés sur l'intervalle d'envoi de 5 min, un
  capteur sain devrait quasi toujours apparaître "récent". Batterie :
  vert ≥ 30 %, orange ≥ 15 %, rouge en dessous. RSSI affiché brut (dBm).

**Vérifications** :
- Backend testé directement (avant déploiement Pi) : MAC connue avec
  RSSI seul → 200 et champ batterie omis ; MAC connue avec RSSI+batterie
  → 200 et les 3 champs présents ; MAC inconnue → 404.
- Script Pi vérifié statiquement (`py_compile`, `black --check`, `ruff`)
  avant déploiement SFTP, puis service redémarré en conditions réelles.
  **Confirmé de bout en bout avec de vraies détections BLE** : les 5
  capteurs vus dans `journalctl` juste après redémarrage
  (`disc-maxi-A03`, `F1A107CCEADE`, `F1C6BE279485`, `Test`, et le
  capteur ELA `P RHT 9078CF`) apparaissent tous dans
  `GET /api/capteurs/hr_t` avec un horodatage cohérent (~13:38 UTC) et
  un RSSI plausible (-50 à -66 dBm) ; les 4 capteurs Blue Maestro
  affichent `derniere_batterie: 100`, le capteur ELA affiche
  `derniere_batterie: null` — comme attendu (limitation du protocole).
- **Rendu visuel non vérifié en navigateur** (pas d'outil de test
  navigateur disponible dans cet environnement) — build/lint frontend
  passent, mais l'affichage réel des pastilles reste à valider par
  l'utilisateur.

**Addendum — décodage batterie ELA en mode Service Data (19/08/2026)** :
question utilisateur sur la possibilité de suivre la batterie du capteur
ELA (limitation notée juste au-dessus). Deux pistes explorées :

1. **Connexion GATT directe (Battery Service standard 0x180F/0x2A19)** —
   testée en conditions réelles sur le capteur ELA du projet
   (`D2:46:BB:82:B7:1C`), deux fois : en fonctionnement normal (timeout
   25s) puis avec le service d'ingestion mis en pause pour écarter une
   contention radio (même timeout). **Écartée** : le capteur n'accepte
   aucune connexion GATT (advertising non-connectable, choix de
   conception courant sur ce type de capteur à batterie).
2. **Champ batterie natif du protocole ELA, via Scan Response** —
   documentation officielle consultée (ELA Innovation, "BLE Frame
   specifications" v11B, section 5 "Battery information", PDF récupéré
   directement, le WebFetch standard étant bloqué en 403 par leur
   serveur). Contrairement à l'hypothèse initiale ("signal binaire"), il
   s'agit d'un **vrai pourcentage** (1 octet brut, comme Blue Maestro),
   transmis dans un bloc Service Data sur l'UUID standard Bluetooth SIG
   **0x2A19** ("Battery Level") — mais **uniquement lorsque la batterie
   réelle est déjà sous 15%** ; rien n'est transmis au-dessus de ce
   seuil (pas de pourcentage "sain" annoncé, à la différence de Blue
   Maestro qui transmet en continu).
   **Implémenté** : nouvelle constante `ELA_UUID_BATTERIE`, décodée dans
   `_decoder_ela_service()` (mode Service Data, celui réellement utilisé
   par le capteur ELA du projet) — même mécanisme exact que
   température/humidité déjà en place, aucun nouveau code de scan
   nécessaire. Volontairement **pas implémenté** pour
   `_decoder_ela_manufacturer()` (mode Manufacturer Specific Data,
   inutilisé sur ce projet) : la batterie y arriverait comme un bloc
   séparé partageant le même company ID (0x0757) que le bloc RHT dans le
   dict `manufacturer_data` de bleak (indexé par company ID) — risque de
   collision non vérifiable sans capteur réellement configuré dans ce
   mode.
   **Limite assumée de la vérification** : le champ n'étant transmis que
   sous 15% de batterie réelle, un test fonctionnel de bout en bout
   n'est pas possible à la demande (pas de moyen de simuler une batterie
   faible sans outil NFC ELA dédié, absent de ce projet). Seul un test
   **structurel** a pu être fait : déployé sur le Pi, service redémarré,
   vérifié sain (aucune erreur dans `journalctl`, capteur ELA toujours
   détecté et décodé normalement), télémétrie confirmée intacte via
   l'API (`derniere_batterie: null`, comportement attendu tant que la
   batterie reste saine). La confirmation fonctionnelle se fera
   naturellement, sans intervention supplémentaire, le jour où un
   capteur ELA franchira réellement ce seuil pendant la campagne.
   **Point d'attention corrigé côté frontend** (question utilisateur
   immédiate) : pour Blue Maestro, `derniere_batterie: null` veut dire
   "jamais reçu" ; pour ELA, ça voudra désormais le plus souvent dire
   "batterie saine, rien à signaler" — même valeur `null`, deux
   significations différentes selon la famille. `etatBatterie()`
   (`Capteurs.jsx`) distingue maintenant les deux cas : pour un capteur
   `famille_capteur === "ela"` ayant déjà au moins une détection
   confirmée (`derniere_detection` non vide), affiche "Saine (> 15 %)"
   (pastille verte) au lieu du "—" ambigu ; reste "—" si le capteur n'a
   jamais été détecté du tout (aucune base pour affirmer quoi que ce
   soit). Déployé (image webapp reconstruite, pod redémarré) et vérifié
   sain (API `/api/capteurs/hr_t` répond 200, logs de démarrage propres)
   — rendu visuel des pastilles toujours non vérifié en navigateur, comme
   pour le reste de cette fonctionnalité.

**Addendum — largeur de page élargie, 1100px → 1400px (19/08/2026)** :
première capture d'écran réelle de la fonctionnalité fournie par
l'utilisateur (tableau "Capteurs HR/T" en conditions réelles) — confirme
le rendu correct des pastilles (RSSI, dernière détection, batterie),
mais révèle deux textes qui retombent sur 2 lignes ("SOCMA 1",
"Saine (> 15 %)") faute de place. Cause : `.app-main` (`index.css`), le
conteneur unique partagé par **toutes** les pages (`App.jsx`, un seul
`<main className="app-main">` autour de `<Routes>`), plafonné à
`max-width: 1100px` — d'où aussi les marges vides de part et d'autre sur
un écran large. Un seul changement de constante affecte donc
uniformément toutes les pages (Vue d'ensemble, Grafana, Export,
Monitoring, etc.), pas seulement Capteurs — confirmé à l'utilisateur
avant de modifier. Relevé à 1400px. Déployé (rebuild image webapp, pod
redémarré) et vérifié sain (API 200). Rendu visuel du nouvel espacement
non revérifié par capture d'écran après coup.

**Addendum — responsive minimal (19/08/2026)** : question utilisateur sur
les tailles d'écran supportées. Audit du code (pas de test navigateur
réel, comme d'habitude) : balise viewport correcte, graphiques SVG
(`viewBox` + `width: 100%`) et nomogrammes canvas (dimensionnés depuis
`clientWidth` au tracé) déjà adaptatifs — mais **aucune media query nulle
part** dans le CSS, barre de navigation (8 onglets + compte) sur une
seule ligne sans repli, et tableaux sans défilement horizontal confiné.
Concrètement desktop-only en dessous d'environ 1024px. Deux correctifs
ciblés, avec accord explicite avant modification (portée app-wide, un
seul conteneur/nav partagé par toutes les pages) :
1. **Tableaux** : nouvelle classe `.tableau-scroll` (`overflow-x: auto`)
   enveloppant chaque `<table>` — seuls deux fichiers en contiennent un
   (`Capteurs.jsx`, `TeneurEau.jsx`), les deux corrigés. Un écran étroit
   fait maintenant défiler le tableau lui-même plutôt que d'écraser les
   colonnes ou toute la page.
2. **Navigation** : bouton hamburger (`menu-bascule`, masqué par défaut,
   visible sous 860px) ajouté dans `App.jsx`, nouvel état `menuOuvert`
   (`useState`). Nav + bloc compte regroupés dans un seul
   `.app-header-panel` replié sous ce seuil (`display: none` sauf classe
   `.ouvert`) plutôt que gérés séparément — évite de dupliquer la logique
   de repli sur deux éléments. Clic sur un lien referme le menu
   automatiquement (`onClick` sur chaque `NavLink`).
Seuil de 860px choisi par estimation (largeur cumulée des 8 onglets +
logo + compte), non calibré par mesure réelle en navigateur. Déployé
(rebuild image webapp, pod redémarré) et vérifié sain (API 200) — comme
pour le reste de cette session, le rendu visuel réel (bascule du menu,
défilement du tableau) n'a pas pu être revérifié faute d'outil de test
navigateur dans cet environnement.

## Points ouverts / non implémentés

- Pas de décodage de la pression (versions 27/43).
- Le token InfluxDB est défini en dur dans les scripts — à injecter via la
  variable d'environnement `INFLUX_TOKEN` ou un secret Kubernetes.
- La configuration GATT s'applique à tous les capteurs Blue Maestro détectés,
  y compris ceux avec `ingestion: false` — comportement intentionnel (optimiser
  la pile avant validation), mais peut être restreint si nécessaire.
- Le namespace Kubernetes `murmetric` est mono-tenant — une isolation par tenant
  (namespace ou cluster dédié) sera nécessaire en mode SaaS multi-clients.
- `secrets.yaml` est à créer manuellement depuis `secrets.yaml.template` et ne
  doit jamais être versionné.
- **Tranché (23/07/2026) : ingestion par lot retenue comme méthode principale,
  pas live.** La licence "Dewesoft NET" nécessaire au streaming (`ingestion_dewesoft.py`,
  DSRemoteConnect) coûte trop cher pour l'usage prévu. `ingestion_dewesoft_dxd.py`
  (dépôt de fichiers `.dxd`) devient donc le chemin d'ingestion **unique**.
  **Câblé le 03/08/2026** : `start_dewesoft.py` lance
  `ingestion_dewesoft_dxd.py`.
  **Tranché définitivement le 04/08/2026** : la méthode live/COM est
  abandonnée pour de bon, pas seulement reléguée en réserve —
  `ingestion_dewesoft.py`, `test_dewesoft_com.py`, `DSRemoteConnect64.dll`/`.dll`
  et la dépendance `pywin32` sont supprimés du dépôt ; `start_dewesoft.py`
  simplifié en conséquence (plus de branche `DEWESOFT_MODE`, un seul chemin
  d'exécution). Cf. section 14 pour l'historique de l'investigation COM/DCOM,
  conservé à titre documentaire.
  Décision d'architecture associée : `ingestion_dewesoft_dxd.py` tourne en
  LOCAL sur le PC labo Windows d'Amiens (même machine que DeweSoftX), pas sur
  le VPS cloud — DeweSoftX dépose/exporte directement dans le dossier
  surveillé, sans Syncthing ni autre composant réseau intermédiaire (une
  mention antérieure de Syncthing dans cette note était trompeuse, corrigée).
  `docker-compose.yml` n'a donc pas besoin de service dédié pour ce chemin
  d'ingestion. Le dossier surveillé (`DXD_WATCH_FOLDER`) reste une variable
  d'environnement (déjà le cas dans `ingestion_dewesoft_dxd.py`) ;
  `start_dewesoft.py` lui fournit une valeur par défaut
  (`DXD_WATCH_FOLDER_DEFAUT = r"C:\MurMetric\depot_dxd"`) uniquement si elle
  n'est pas déjà définie dans l'environnement — changer de dossier à la mise
  en prod ne demande donc de toucher qu'un seul réglage (variable
  d'environnement, prioritaire, ou cette constante).
  **Dossier réel confirmé le 04/08/2026** :
  `C:\Users\Public\Documents\Dewesoft\Data` (dossier public généré par
  DeweSoftX lui-même, pas un dossier créé pour MurMetric) —
  `DXD_WATCH_FOLDER_DEFAUT` mis à jour en conséquence. Choix assumé de
  laisser `ingestion_dewesoft_dxd.py` déplacer les fichiers traités/en
  erreur dans des sous-dossiers `traites/`/`erreurs/` **à l'intérieur** de
  ce dossier DeweSoft plutôt que vers un emplacement séparé — DeweSoftX ne
  semble pas dépendre de la présence continue des fichiers exportés dans ce
  dossier pour son propre fonctionnement, mais si un comportement DeweSoft
  imprévu apparaît suite à cette réorganisation, envisager de rediriger
  `DXD_PROCESSED_FOLDER`/`DXD_ERROR_FOLDER` vers un dossier hors de
  `Public\Documents\Dewesoft\Data`.
- **Bug critique corrigé (03/08/2026) : `kafka-0` en `CrashLoopBackOff`
  permanent sur le déploiement Kubernetes local (namespace `murmetric`,
  actif depuis son premier déploiement — 780 redémarrages sur 10 jours,
  jamais fonctionnel).** Découvert en testant l'ingestion `.dxd` de bout en
  bout : le `Service` Kubernetes `kafka` (`k8s/kafka/service.yaml`)
  n'exposait que le port 9092 (PLAINTEXT), jamais le 9093 (CONTROLLER) —
  or `KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093` (`k8s/kafka/statefulset.yaml`)
  exige que le broker joigne le contrôleur via `kafka:9093` pour
  s'auto-enregistrer, même en cluster à 1 seul nœud (broker+controller
  combinés). Le port n'étant jamais routé par le `Service`, l'enregistrement
  échouait systématiquement au bout de ~55s ("unable to register with the
  controller quorum"), provoquant le crash-loop. Un correctif antérieur
  (`KAFKA_HEAP_OPTS` borné, déjà présent dans le manifeste) traitait un
  symptôme voisin mais ne pouvait pas résoudre celui-ci. Conséquence en
  cascade : le bridge MQTT→Kafka et le kafka-consumer-influx étaient eux
  aussi bloqués en boucle de reconnexion infinie depuis le premier
  déploiement — **aucune donnée n'a jamais transité par ce pipeline
  Kubernetes avant ce correctif** (bucket InfluxDB vérifié vide). Corrigé en
  ajoutant le port 9093 (nommé `controller`) au `Service` ; `kafka-0`
  démarre maintenant proprement (0 redémarrage sur 2m30 observées), le
  bridge et le consumer se reconnectent, et un test d'ingestion réel (fichier
  `.dxd` du 23/06/2026, canal HA1, 432 000 mesures) a été vérifié bout en
  bout jusqu'à InfluxDB (valeurs identiques à la lecture SDK directe du
  fichier source). Kubernetes est destiné à être intégré au projet pour la
  scalabilité (nombre croissant de parois/capteurs) — ce correctif était
  donc bloquant pour cet objectif, pas seulement pour un test isolé.
- Grafana **est** intégré (`docker-compose.yml` + `k8s/grafana/`, datasource
  InfluxDB préconfigurée) — corriger cette note si elle traîne ailleurs comme
  point ouvert.
- **Exigence produit pour le rendu final (28/07/2026) : Grafana embarqué dans
  l'interface utilisateur**, pas seulement disponible comme outil séparé —
  l'utilisateur doit pouvoir choisir, depuis l'appli, entre la visualisation
  maison (ex. l'abaque 3D) et des panneaux/dashboards Grafana natifs, sans
  changer d'onglet/outil. Prérequis technique identifié mais non traité :
  embarquer un dashboard Grafana (typiquement en `<iframe>`) demande
  d'activer `allow_embedding` côté configuration Grafana, et de régler
  l'authentification de l'iframe (accès anonyme en lecture seule sur les
  dashboards partagés, ou un proxy d'authentification) — sinon l'utilisateur
  devrait se reconnecter séparément à l'intérieur de l'iframe. Non
  implémenté ; à traiter au moment de construire l'interface applicative
  réelle (l'abaque 3D reste un POC autonome, cf. section 18).


