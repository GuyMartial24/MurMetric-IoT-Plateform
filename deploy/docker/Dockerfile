FROM python:3.12-slim

# Ce Dockerfile cible le déploiement côté Raspberry Pi :
#   - ingestion BLE (ingestion_capteurs_bluetooth.py)
#   - bridge MQTT → InfluxDB (bridge_mqtt_to_influx.py)
#
# Build depuis la racine du dépôt (comme les autres Dockerfile*) :
#   docker build -t murmetric-rpi -f deploy/docker/Dockerfile .
#
# Pour l'accès au Bluetooth hardware du RPi, lancer avec :
#   docker run --net=host --privileged \
#     -e MQTT_BROKER=<ip_vps> -e INFLUX_TOKEN=<token> \
#     murmetric-rpi
#
# L'ingestion DeweSoftX (PC Windows) est gérée séparément
# par start_dewesoft.py sur le PC labo.

WORKDIR /app

COPY ingestion/raspberry_pi/requirements-rpi.txt ./
RUN pip install --no-cache-dir -r requirements-rpi.txt

# capteurs.example.json (données factices) sert de base — en production,
# reconstruire avec le vrai capteurs.json (gitignored) à la place, même
# mécanisme que Dockerfile.webapp.
COPY ingestion/raspberry_pi/capteurs.example.json ./capteurs.json
COPY ingestion/raspberry_pi/configure_capteurs.py ./
COPY ingestion/raspberry_pi/ingestion_capteurs_bluetooth.py ./
COPY pipeline/bridge_mqtt_to_influx.py ./
COPY ingestion/raspberry_pi/start.py ./

CMD ["python", "-u", "start.py"]