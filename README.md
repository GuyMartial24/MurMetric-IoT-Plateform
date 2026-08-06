# MurMetric — Plateforme IoT de monitoring métrologique des parois biosourcées

> Plateforme d'acquisition, de transport et de visualisation en temps réel
> des données issues de capteurs embarqués dans des parois à base de matériaux
> biosourcés (chanvre-chaux, paille, etc.) — **FRD-CODEM**

---

## Sommaire

1. [Contexte et problématique](#1-contexte-et-problématique)
2. [Enjeux du projet](#2-enjeux-du-projet)
3. [Contraintes techniques](#3-contraintes-techniques)
4. [Architecture générale](#4-architecture-générale)
5. [Capteurs utilisés](#5-capteurs-utilisés)
6. [Choix technologiques](#6-choix-technologiques)
7. [Structure du projet](#7-structure-du-projet)
8. [Installation et déploiement](#8-installation-et-déploiement)
9. [Configuration des capteurs](#9-configuration-des-capteurs)
10. [Déploiement Kubernetes (SaaS)](#10-déploiement-kubernetes-saas)
11. [Auteur et organisation](#11-auteur-et-organisation)

---

## 1. Contexte et problématique

### Le défi des matériaux biosourcés

Le secteur du bâtiment est l'un des principaux émetteurs de CO₂ en France.
Face à l'urgence climatique, les matériaux biosourcés — chanvre-chaux,
paille, ouate de cellulose — connaissent un essor important comme alternatives
aux isolants synthétiques. Ils présentent d'excellentes propriétés thermiques,
hygrométriques et un bilan carbone favorable.

Cependant, leur comportement hygro-thermique dans le temps reste peu documenté
à grande échelle : comment évolue la teneur en eau d'une paroi en chanvre-chaux
après sa mise en œuvre ? Quelles sont les cinétiques de séchage selon l'exposition
(nord/sud/est/ouest) et les conditions climatiques locales ? Comment la paroi
se comporte-t-elle en phase de retrait ?

Ces questions conditionnent la durabilité, la performance énergétique et la
qualité sanitaire des bâtiments biosourcés. Elles restent difficiles à répondre
sans un monitoring continu, à long terme, **in situ**.

### La problématique de MurMetric

> **Comment collecter, transporter et exploiter en continu les données
> métrologique (température, humidité, retrait) de centaines de capteurs
> coulés dans des parois de chantiers dispersés géographiquement, tout en
> garantissant la robustesse, la scalabilité et la pérennité du système ?**

Les défis sont multiples :
- Les capteurs sont **physiquement intégrés dans la matière** (coulés dans le
  béton de chanvre) — ils ne peuvent pas être retirés pour maintenance.
- Le signal BLE doit **traverser la paroi** depuis l'intérieur.
- Les chantiers sont **dispersés géographiquement** (Troyes, Amiens, etc.) et
  ne disposent pas toujours d'une infrastructure réseau stable.
- La flotte peut atteindre **200+ capteurs** sur un même site.
- Les données doivent être **exploitables sur le long terme** (années) pour
  observer les cinétiques de vieillissement.

---

## 2. Enjeux du projet

| Dimension | Enjeu |
|---|---|
| **Scientifique** | Constituer une base de données métrologique de référence sur le comportement des parois biosourcées dans le temps |
| **Technique** | Concevoir un pipeline IoT fiable, résilient et scalable pour 200+ capteurs BLE sur plusieurs sites |
| **Industriel** | Poser les bases d'une offre SaaS déployable chez d'autres maîtres d'ouvrage et bureaux d'études |
| **Environnemental** | Contribuer à la documentation scientifique et à la démocratisation des matériaux biosourcés |
| **Pédagogique** | Monter en compétences sur les technologies IoT, streaming de données (Kafka) et orchestration (Kubernetes) |

---

## 3. Contraintes techniques

### Capteurs

- **Intégration physique** : les capteurs Blue Maestro Disc Maxi sont coulés dans
  la paroi — aucune connexion filaire possible, aucun remplacement envisageable
  sans détruire la paroi.
- **Autonomie critique** : la pile (CR2477) doit durer **4 à 5 ans**. L'intervalle
  de log est réglé au maximum (86 400 s / 24 h) pour minimiser la consommation.
- **Signal BLE à travers la paroi** : le signal doit traverser plusieurs
  centimètres de béton de chanvre depuis l'intérieur — la portée effective
  est réduite (< 10 m en conditions réelles).
- **Protocole passif** : l'ingestion BLE utilise exclusivement la **publicité passive**
  (advertising) — aucune connexion GATT n'est établie en ingestion pour préserver
  la batterie. La connexion GATT n'est utilisée qu'à la configuration initiale.

### Réseau

- **Terrain instable** : les chantiers peuvent avoir des connexions internet
  intermittentes (4G, fibre non déployée). Le système doit survivre aux coupures
  sans perte de données.
- **Multi-sites** : chaque site dispose de sa propre passerelle (Raspberry Pi),
  toutes publient vers un broker MQTT cloud centralisé.

### Données

- **Double fréquence** : les capteurs BLE publient toutes les 24 h (basse
  fréquence), tandis que les capteurs de retrait DeweSoftX émettent à **1 mesure/s**
  (haute fréquence) — le pipeline doit absorber les deux débits sans saturation.
- **Long terme** : les données doivent être conservées plusieurs années pour
  l'analyse des cinétiques de vieillissement.

---

## 4. Architecture générale

```
┌─────────────────────────────────────────────────────────────────────────┐
│  TERRAIN — Site Amiens (exemple)                                        │
│                                                                         │
│  Parois instrumentées                                                   │
│  ┌──────────────┐   BLE advertising                                     │
│  │ Disc Maxi ×N │ ──────────────────► Raspberry Pi 5                   │
│  │ (T°, HR%)    │   passive scan       start.py                        │
│  └──────────────┘   company ID filter  ingestion_capteurs_bluetooth.py  │
│                                        configure_capteurs.py            │
│                                              │                          │
│  ┌──────────────┐   Export .dxd               │                          │
│  │ Capteur      │ ──────────────────► PC labo Windows                  │
│  │ retrait (×N) │   1 msg/s (dépôt fichier)   start_dewesoft.py         │
│  └──────────────┘                    ingestion_dewesoft_dxd.py          │
│                                              │                          │
│                           SQLite local  ◄────┤ Si VPS inaccessible      │
│                           (murmetric_buffer.db) └── republication auto  │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │ MQTT (TLS, port 8883)
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  CLOUD VPS — docker-compose / Kubernetes                                │
│                                                                         │
│  ┌─────────────┐    ┌──────────────────────┐    ┌──────────────────┐   │
│  │  Mosquitto  │───►│ bridge_mqtt_to_kafka  │───►│     Kafka        │   │
│  │  MQTT broker│    │ (transfère sans       │    │ rétention 7 j    │   │
│  └─────────────┘    │  transformer)         │    │ 3 topics/tenant  │   │
│                     └──────────────────────┘    └────────┬─────────┘   │
│                                                          │              │
│                     ┌──────────────────────┐             │              │
│                     │ kafka_consumer_influx │◄────────────┘              │
│                     │ batch async 500pts/1s │                            │
│                     └──────────┬───────────┘                            │
│                                │                                         │
│                     ┌──────────▼───────────┐    ┌──────────────────┐   │
│                     │      InfluxDB 2.7    │───►│     Grafana       │   │
│                     │  mesures_capteurs    │    │  Dashboards       │   │
│                     │  registre_capteurs   │    │  alertes          │   │
│                     │  mesures_dewesoft    │    └──────────────────┘   │
│                     └──────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────┘
```

### Résilience sur deux segments

| Segment | Mécanisme | Scénario couvert |
|---|---|---|
| Terrain → VPS | SQLite local + republication auto | Coupure réseau, VPS inaccessible |
| Mosquitto → InfluxDB | Kafka (rétention 7 jours) | Crash InfluxDB, redémarrage consumer |

---

## 5. Capteurs utilisés

### Blue Maestro Disc Maxi (température + humidité)

| Caractéristique | Valeur |
|---|---|
| Modèle | Disc Maxi v42 (disc-maxi-alerts-003) |
| Mesures | Température (−40/+120 °C, ±0.3 °C) + Humidité (0−100 %, ±2 %) |
| Pile | CR2477 (4 à 5 ans d'autonomie) |
| Protocole | BLE 4.2 — publicité passive (advertising) |
| Intervalle de log | 60 s à 86 400 s (réglé à 86 400 s / 24 h par MurMetric) |
| Dimensions | 37.5 × 37.5 × 14.3 mm |
| Particularité | **Peut être coulé dans des parois en matériaux biosourcés** |

Les capteurs sont configurés via GATT BLE à la commande `setlog~86400` pour
maximiser l'autonomie. Cette configuration est automatique au démarrage via
`configure_capteurs.py` et persistée dans `capteurs.json`.

### Capteurs de retrait (DeweSoftX)

- Acquisition par **import de fichiers .dxd** déposés/exportés par DeweSoftX
  dans un dossier surveillé, lus via la librairie officielle **DWDataReader**
  (SDK vendored, ctypes)
- **Fréquence : 1 mesure/seconde par canal** (haute fréquence)
- Données publiées sur le topic MQTT `frd/dewesoft/bruts`

---

## 6. Choix technologiques

### Python

Écosystème riche pour l'IoT, excellent support BLE (`bleak`), asyncio natif
pour la gestion concurrente des tâches (scan, sync SQLite, reconfiguration
périodique). Multiplateforme (Linux/RPi + Windows/PC labo).

### BLE passif (advertising)

L'ingestion utilise exclusivement le **mode passif** (scan advertising) —
aucune connexion GATT pendant la collecte de données. Ce choix :
- Préserve la batterie des capteurs (pas de wake-up pour connexion)
- Permet de monitorer des centaines de capteurs simultanément
- Élimine les timeouts de connexion
La connexion GATT active n'est établie qu'à la configuration initiale
(`setlog~86400`) via `configure_capteurs.py`.

### MQTT (paho-mqtt)

Protocole IoT léger, adapté aux connexions instables (QoS 1 = at-least-once).
Standard de fait pour les architectures IoT multi-sources.

### Apache Kafka (kafka-python)

Introduit pour répondre à deux besoins :

1. **Débit** : les capteurs de retrait émettent à 1 msg/s — le pipeline doit
   absorber ce débit sans saturer l'écriture InfluxDB. Le mode batch async
   (500 pts / flush 1 s) du consumer Kafka y répond.

2. **Découplage** : chaque consommateur (InfluxDB, alertes futures, export ML)
   lit les topics Kafka indépendamment, sans modifier le bridge. Nouveau
   consommateur = nouveau service, pas de modification du code existant.

3. **Résilience** : rétention 7 jours — si InfluxDB redémarre, le consumer
   reprend depuis le dernier offset sans perte.

4. **Multi-tenant SaaS** : topics namespaced par tenant
   (`murmetric.{tenant}.capteurs.bruts`) pour isoler les données de chaque client.

### InfluxDB 2.7

Base de données time-series optimisée pour les données horodatées. Modèle
tags/fields adapté aux métadonnées capteurs (adresse MAC, emplacement, prestation).
Trois mesures : `mesures_capteurs`, `registre_capteurs`, `mesures_dewesoft`.

### SQLite

Buffer local store-and-forward sans dépendance externe. Présent sur le RPi
et le PC Windows, indépendant sur chaque machine. Gère les coupures réseau
terrain sans perte de données.

### Docker + docker-compose

Conteneurisation de la stack VPS (5 services). Reproductibilité du déploiement,
isolation des dépendances, `restart: unless-stopped` pour la haute disponibilité.

### Kubernetes (k3s recommandé)

Orchestration pour le passage en mode **SaaS multi-clients** :
- Déploiement automatisé pour chaque nouveau client
- Scaling horizontal du consumer Kafka (`--replicas=N`)
- Rolling updates sans interruption de service
- Isolation par namespace ou cluster selon le niveau de tenancy requis

---

## 7. Structure du projet

```
MurMetric-IoT-Plateform/
│
├── 📄 start.py                          # Lanceur RPi (BLE uniquement)
├── 📄 start_dewesoft.py                 # Lanceur PC Windows (DeweSoftX)
│
├── 📄 configure_capteurs.py             # Configuration GATT BLE (setlog~86400)
├── 📄 ingestion_capteurs_bluetooth.py   # Ingestion BLE passive (scan permanent)
├── 📄 ingestion_dewesoft_dxd.py         # Ingestion DeweSoftX par dépôt de fichiers .dxd
│
├── 📄 bridge_mqtt_to_kafka.py           # VPS : MQTT → Kafka (3 topics)
├── 📄 kafka_consumer_influx.py          # VPS : Kafka → InfluxDB (batch async)
├── 📄 bridge_mqtt_to_influx.py          # Legacy : MQTT → InfluxDB (tests locaux)
│
├── 📄 capteurs.json                     # Registre des capteurs BLE (hot-reload)
├── 📄 mosquitto.conf                    # Configuration du broker MQTT
│
├── 📄 requirements.txt                  # Référence globale toutes dépendances
├── 📄 requirements-rpi.txt              # RPi : bleak + paho-mqtt
├── 📄 requirements-windows.txt          # PC Windows : paho-mqtt
├── 📄 requirements-vps.txt              # VPS : paho-mqtt + kafka-python + influxdb-client
│
├── 📄 Dockerfile                        # Image RPi (déploiement Docker optionnel)
├── 📄 Dockerfile.bridge                 # Image VPS bridge MQTT → Kafka
├── 📄 Dockerfile.kafka-consumer         # Image VPS consumer Kafka → InfluxDB
├── 📄 docker-compose.yml                # Stack VPS complète (5 services)
│
├── 📁 k8s/                              # Manifests Kubernetes
│   ├── namespace.yaml
│   ├── secrets.yaml.template            # ⚠️ Copier → secrets.yaml (non versionné)
│   ├── mosquitto/  (configmap, deployment, service)
│   ├── kafka/      (statefulset, service — 10 Gi PVC)
│   ├── influxdb/   (statefulset, service, pvc — 20 Gi)
│   ├── bridge-mqtt-kafka/  (deployment, configmap)
│   └── kafka-consumer-influx/  (deployment — scalable)
│
└── 📄 logique_projet.md                 # Documentation technique détaillée
```

---

## 8. Installation et déploiement

### Prérequis

- Python 3.12+
- Docker + Docker Compose (VPS)
- Git

### Raspberry Pi (terrain — ingestion BLE)

```bash
git clone https://github.com/GuyMartial24/MurMetric-IoT-Plateform.git
cd MurMetric-IoT-Plateform
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-rpi.txt

# Configurer l'adresse du broker MQTT cloud
export MQTT_BROKER=<ip_vps>

# Lancer la plateforme
python start.py
```

### PC labo Windows (terrain — DeweSoftX)

```powershell
git clone https://github.com/GuyMartial24/MurMetric-IoT-Plateform.git
cd MurMetric-IoT-Plateform
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements-windows.txt

# Configurer l'adresse du broker MQTT cloud
$env:MQTT_BROKER = "<ip_vps>"

# Lancer l'ingestion
python start_dewesoft.py
```

### VPS Cloud (stack complète)

```bash
git clone https://github.com/GuyMartial24/MurMetric-IoT-Plateform.git
cd MurMetric-IoT-Plateform

# Définir le token InfluxDB
export INFLUX_TOKEN=<mon_token_influxdb>

# Construire et démarrer les 5 services
docker compose up -d --build

# Vérifier l'état des services
docker compose ps
docker compose logs -f kafka-consumer
```

### Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `MQTT_BROKER` | `localhost` | Adresse du broker MQTT cloud |
| `MQTT_PORT` | `1883` | Port MQTT |
| `INFLUX_TOKEN` | — | Token API InfluxDB (**obligatoire en production**) |
| `INFLUX_URL` | `http://localhost:8086` | URL InfluxDB |
| `INFLUX_ORG` | `FRD_CODEM` | Organisation InfluxDB |
| `INFLUX_BUCKET` | `Test_Capteurs` | Bucket InfluxDB |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Serveurs Kafka |
| `TENANT_ID` | `frd` | Identifiant tenant (namespacing Kafka) |
| `RECONF_INTERVAL` | `21600` | Intervalle de reconfiguration GATT (s) |
| `SQLITE_RETENTION` | `7` | Rétention du buffer local (jours) |
| `POLL_INTERVAL` | `1.0` | Intervalle de lecture DeweSoftX (s) |

---

## 9. Configuration des capteurs

Les capteurs BLE sont gérés via `capteurs.json`, un fichier JSON avec
**hot-reload** : toute modification est prise en compte sans redémarrage.

### Structure d'une entrée

```json
{
  "D2:0D:27:1C:F3:97": {
    "mac": "D2:0D:27:1C:F3:97",
    "nom": "disc-maxi-A03",
    "emplacement": "Atelier Troyes — Paroi Nord",
    "latitude": 48.2973,
    "longitude": 4.0744,
    "altitude_m": 112.5,
    "prestation": "C10517",
    "categorie R&D": "Hygrothermal",
    "ingestion": true,
    "lint_configure": true,
    "lint_max_confirme_s": 86400.0
  }
}
```

### Workflow d'ajout d'un nouveau capteur

1. **Détection automatique** : tout capteur Blue Maestro (company ID `0x0133`)
   non présent dans `capteurs.json` est auto-enregistré avec `"ingestion": false`.
2. **Validation opérateur** : éditer `capteurs.json`, renseigner `nom` et
   `emplacement`, passer `"ingestion": true`.
3. **Hot-reload** : la modification est active dès le prochain paquet BLE reçu,
   **sans redémarrage**.
4. **Configuration automatique** : `configure_capteurs.py` détecte que
   `lint_configure` est absent et applique `setlog~86400` au prochain cycle.

### Registre InfluxDB (`registre_capteurs`)

À chaque connexion MQTT, l'intégralité du `capteurs.json` est publiée sur le
topic `frd/capteurs/registre` et stockée dans la mesure `registre_capteurs`
d'InfluxDB. Les applications clientes peuvent ainsi interroger les métadonnées
(GPS, prestation, catégorie R&D) en même temps que les mesures.

---

## 10. Déploiement Kubernetes (SaaS)

La cible à long terme de MurMetric est une **offre SaaS** permettant à d'autres
maîtres d'ouvrage, bureaux d'études et laboratoires de déployer la même
infrastructure de monitoring pour leurs propres chantiers.

### Déploiement sur un cluster k3s (VPS unique)

```bash
# Installer k3s
curl -sfL https://get.k3s.io | sh -

# Préparer les secrets
cp k8s/secrets.yaml.template k8s/secrets.yaml
# Éditer k8s/secrets.yaml : encoder les valeurs en base64
#   echo -n "mon_token" | base64

# Déployer la stack
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets.yaml
kubectl apply -f k8s/mosquitto/
kubectl apply -f k8s/kafka/
kubectl apply -f k8s/influxdb/
kubectl apply -f k8s/bridge-mqtt-kafka/
kubectl apply -f k8s/kafka-consumer-influx/

# Vérifier les pods
kubectl get pods -n murmetric

# Scaler le consumer si le débit augmente
kubectl scale deployment kafka-consumer-influx --replicas=3 -n murmetric
```

### Isolation multi-tenant

Chaque client SaaS dispose d'un `TENANT_ID` unique. Les topics Kafka sont
automatiquement namespaced (`murmetric.{tenant}.capteurs.bruts`) pour garantir
l'isolation des données entre clients.

---

## 11. Auteur et organisation

### Auteur

**Martial GADJEUKAMENI**
Ingénieur R&D — FRD-CODEM
✉️ guymg33@gmail.com
🔗 [GitHub](https://github.com/GuyMartial24)

### Organisation

**FRD-CODEM** — Bureau d'études spécialisé dans la construction durable
et les matériaux biosourcés. Accompagne maîtres d'ouvrage et industriels
dans la conception, le suivi et l'évaluation de bâtiments à haute performance
environnementale.

### Technologies

![Python](https://img.shields.io/badge/Python-3.12-blue)
![MQTT](https://img.shields.io/badge/MQTT-paho--mqtt-orange)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.7-black)
![InfluxDB](https://img.shields.io/badge/InfluxDB-2.7-purple)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Kubernetes](https://img.shields.io/badge/Kubernetes-k3s-blue)
![BLE](https://img.shields.io/badge/BLE-bleak-lightblue)

---

*Documentation technique détaillée : voir [logique_projet.md](logique_projet.md)*
