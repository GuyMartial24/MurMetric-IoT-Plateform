"""Configuration centralisée — mêmes variables d'environnement que le reste
du pipeline (backfill_hr_t.py, backfill_teneur_eau.py, kafka_consumer_influx.py)."""
import os
from pathlib import Path

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "MON_TOKEN_API_GENERE_PAR_INFLUXDB")
INFLUX_ORG = os.getenv("INFLUX_ORG", "FRD_CODEM")
# "Test_Capteurs", pas "Capteurs" : c'est le nom réel du bucket sur le VPS
# (vérifié le 12/08/2026 via `influx bucket list` — le bucket "Capteurs"
# n'existe pas). Une note de logique_projet.md section 27 affirmait un
# renommage Test_Capteurs → Capteurs, jamais réellement appliqué côté k8s
# (murmetric-config y référence toujours Test_Capteurs) — doc corrigée en
# conséquence, cf. section 32.
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Test_Capteurs")

# Groq (API OpenAI-compatible, https://api.groq.com/openai/v1) — retenu
# plutôt qu'Anthropic (identifiants fournis par l'utilisateur le 12/08/2026,
# app "MurMetric_AI" sur la console Groq). Ne change rien à la justification
# de section 32 (API cloud plutôt que LLM local sur le VPS partagé) : seul
# le fournisseur change, pas l'architecture (tool use, garde-fou anti-données-
# brutes, etc.).
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# Authentification JWT (section 32, "Non tranché à ce stade" -> implémenté
# le 12/08/2026). Secret de signature — à définir via JWT_SECRET_KEY en
# production (k8s secret) ; le défaut ci-dessous n'est là que pour le dev
# local, ne jamais le laisser tel quel en prod.
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "dev-secret-a-changer-en-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HEURES = 24

# Bootstrap du tout premier compte (users.json vide) — variables d'env
# plutôt qu'un identifiant en dur, même logique que GF_SECURITY_ADMIN_PASSWORD
# pour Grafana (k8s/grafana/deployment.yaml).
ADMIN_BOOTSTRAP_USERNAME = os.getenv("ADMIN_BOOTSTRAP_USERNAME", "")
ADMIN_BOOTSTRAP_PASSWORD = os.getenv("ADMIN_BOOTSTRAP_PASSWORD", "")

# Fichier des comptes utilisateurs — doit survivre aux redéploiements
# (rebuild d'image), donc monté depuis un volume persistant en production
# (k8s/webapp/pvc.yaml), pas copié dans l'image comme capteurs.json.
USERS_JSON = Path(os.getenv("USERS_DIR", str(Path(__file__).resolve().parent))) / "users.json"

# Chantier "source unique" (section 32, 13/08/2026) : capteurs.json/
# capteurs_retrait.json cessent d'être des copies figées dans l'image Docker
# pour devenir des fichiers vivants sur le même volume persistant que
# users.json (USERS_DIR=/data en prod, cf. k8s/webapp/deployment.yaml) —
# la webapp devient la source de vérité, le PC Amiens
# (ingestion_dewesoft_dxd.py) et le Pi (ingestion_capteurs_bluetooth.py)
# interrogent désormais son API au lieu de leur copie locale. Auparavant les
# trois copies (dépôt git, PC Amiens/Pi, image webapp) divergeaient
# silencieusement — seules les copies PC Amiens/Pi comptaient réellement pour
# l'étiquetage des mesures en direct.
#
# En dev local (dépôt git complet, pas de USERS_DIR), on continue de lire/
# écrire directement les fichiers du dépôt à la racine — pas de distinction
# volume/image nécessaire hors production.
_ICI = Path(__file__).resolve()
_PARENTS = _ICI.parents
_RACINE_DEPOT_LOCAL = _PARENTS[3] if len(_PARENTS) > 3 else None
_UTILISER_RACINE_LOCALE = _RACINE_DEPOT_LOCAL is not None and (_RACINE_DEPOT_LOCAL / "capteurs.json").exists()
_VOLUME_PERSISTANT = os.getenv("USERS_DIR")
CAPTEURS_DIR = Path(_VOLUME_PERSISTANT) if _VOLUME_PERSISTANT else Path(
    os.getenv("CAPTEURS_DIR", str(_RACINE_DEPOT_LOCAL if _UTILISER_RACINE_LOCALE else _ICI.parent))
)
CAPTEURS_JSON = CAPTEURS_DIR / "capteurs.json"
CAPTEURS_RETRAIT_JSON = CAPTEURS_DIR / "capteurs_retrait.json"

# Copies "amorce" (seed) baties dans l'image Docker (Dockerfile.webapp) —
# ne servent qu'à initialiser le volume persistant à son tout premier
# démarrage (cf. main.py, _amorcer_capteurs) ; jamais relues ensuite, jamais
# écrasées par une modification faite depuis l'interface.
CAPTEURS_JSON_SEED = _ICI.parent / "capteurs.seed.json"
CAPTEURS_RETRAIT_JSON_SEED = _ICI.parent / "capteurs_retrait.seed.json"

# Secret partagé pour les endpoints machine-à-machine (POST .../enregistrer)
# appelés sans session utilisateur par ingestion_dewesoft_dxd.py (PC Amiens)
# et ingestion_capteurs_bluetooth.py (Pi) pour déclarer un nouveau canal/MAC
# inconnu — même logique que JWT_SECRET_KEY/ADMIN_BOOTSTRAP_PASSWORD (env var
# en dev, k8s secret en prod). Vide = endpoints d'enregistrement désactivés
# (404), pour ne jamais les exposer sans protection par défaut.
INGESTION_API_KEY = os.getenv("INGESTION_API_KEY", "")

