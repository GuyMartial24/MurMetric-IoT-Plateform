#!/bin/sh
# Génère mosquitto_password.txt (fichier de mots de passe Mosquitto, format
# bcrypt) à partir de MQTT_USERNAME/MQTT_PASSWORD dans .env — MurMetric/FRD-CODEM.
#
# À exécuter UNE FOIS avant le premier `docker compose up` (et à nouveau si le
# mot de passe change). mosquitto_password.txt est gitignored — jamais commité.
# Utilise `docker run` avec l'image eclipse-mosquitto elle-même (contient déjà
# l'outil mosquitto_passwd), donc aucune installation locale supplémentaire
# n'est nécessaire, y compris sur le PC labo Windows/RPi.
#
# Usage :
#   ./generer_mosquitto_password.sh
#   (lit MQTT_USERNAME/MQTT_PASSWORD depuis .env dans le répertoire courant)

set -e

# Évite que Git Bash (MSYS) sur Windows ne réinterprète "/out" (chemin CÔTÉ
# CONTENEUR du -v ci-dessous) comme un chemin Windows à traduire — sans effet
# sur Linux/le VPS, où cette variable n'a aucune signification.
export MSYS_NO_PATHCONV=1

if [ ! -f .env ]; then
  echo "❌ .env introuvable — copiez .env.example en .env et renseignez MQTT_USERNAME/MQTT_PASSWORD d'abord."
  exit 1
fi

MQTT_USERNAME=$(grep -E '^MQTT_USERNAME=' .env | cut -d '=' -f2-)
MQTT_PASSWORD=$(grep -E '^MQTT_PASSWORD=' .env | cut -d '=' -f2-)

if [ -z "$MQTT_USERNAME" ] || [ -z "$MQTT_PASSWORD" ]; then
  echo "❌ MQTT_USERNAME et/ou MQTT_PASSWORD absents ou vides dans .env."
  exit 1
fi

docker run --rm -v "$(pwd):/out" eclipse-mosquitto:2.0 \
  mosquitto_passwd -b -c /out/mosquitto_password.txt "$MQTT_USERNAME" "$MQTT_PASSWORD"

# mosquitto_passwd crée le fichier appartenant à root (le conteneur qui l'a
# généré tourne en root) — le monter en lecture seule dans le conteneur
# mosquitto empêche ensuite son propre entrypoint de corriger les
# permissions pour l'utilisateur non-root sous lequel il tourne réellement
# ("Unable to open pwfile", observé le 04/08/2026). Le contenu n'est qu'un
# hash salé (pas le mot de passe en clair), rendre le fichier lisible par
# tous est donc un compromis acceptable. Sur un vrai Linux (VPS), le fichier
# appartient réellement à root et un `chmod` simple échoue ("Operation not
# permitted") — repli sur `sudo` dans ce cas (observé le 04/08/2026 sur le
# VPS Oracle) ; sur Docker Desktop/Windows, le fichier apparaît déjà
# possédé par l'utilisateur courant, `chmod` simple suffit.
chmod 644 mosquitto_password.txt 2>/dev/null || sudo chmod 644 mosquitto_password.txt
chown "$(id -u):$(id -g)" mosquitto_password.txt 2>/dev/null || sudo chown "$(id -u):$(id -g)" mosquitto_password.txt 2>/dev/null || true

echo "✅ mosquitto_password.txt généré pour l'utilisateur '$MQTT_USERNAME'."
echo "   Relancez 'docker compose up -d mosquitto' pour appliquer."
