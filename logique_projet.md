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
```

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
| `latitude` | ✅ | Latitude GPS en degrés décimaux (`null` si inconnue) |
| `longitude` | ✅ | Longitude GPS en degrés décimaux (`null` si inconnue) |
| `altitude_m` | ✅ | Altitude en mètres NGF (`null` si inconnue) |
| `prestation` | ✅ | Référence de la prestation ou du contrat (ex. `C10517`) |
| `categorie R&D` | ✅ | Catégorie R&D associée (ex. `Hygrothermal`, `Retrait`, `Thermique`) |
| `ingestion` | ✅ | `true` = mesures publiées sur MQTT ; `false` (défaut) = exclu |
| `lint_configure` | ❌ auto | Positionné par `configure_capteurs.py` après succès GATT |
| `lint_max_confirme_s` | ❌ auto | Valeur d'intervalle de log confirmée (secondes) |

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
| `prestation` | Tag | Référence de la prestation ou du contrat |
| `categorie R&D` | Tag | Catégorie R&D associée |
| `ingestion` | Field bool | true = mesures publiées sur MQTT |
| `lint_configure` | Field bool | true = intervalle de log optimisé |
| `lint_max_confirme_s` | Field float | Intervalle de log confirmé (secondes) |
| `latitude` | Field float | Latitude GPS (si renseignée) |
| `longitude` | Field float | Longitude GPS (si renseignée) |
| `altitude_m` | Field float | Altitude NGF en mètres (si renseignée) |

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
| `valeur` | Field float | Valeur de la mesure de retrait |
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

## Points ouverts / non implémentés

- Pas de décodage de la pression (versions 27/43).
- Le token InfluxDB est défini en dur dans les scripts — à injecter via la
  variable d'environnement `INFLUX_TOKEN` ou un secret Kubernetes.
- La configuration GATT s'applique à tous les capteurs Blue Maestro détectés,
  y compris ceux avec `ingestion: false` — comportement intentionnel (optimiser
  la pile avant validation), mais peut être restreint si nécessaire.
- Grafana n'est pas encore inclus dans `docker-compose.yml` ni dans les manifests
  Kubernetes — à ajouter avec une datasource InfluxDB préconfigurée.
- Le namespace Kubernetes `murmetric` est mono-tenant — une isolation par tenant
  (namespace ou cluster dédié) sera nécessaire en mode SaaS multi-clients.
- `secrets.yaml` est à créer manuellement depuis `secrets.yaml.template` et ne
  doit jamais être versionné.


