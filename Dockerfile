FROM python:3.12-slim

# Ce Dockerfile cible le déploiement côté Raspberry Pi :
#   - ingestion BLE (ingestion_capteurs_bluetooth.py)
#   - bridge MQTT → InfluxDB (bridge_mqtt_to_influx.py)
#
# Pour l'accès au Bluetooth hardware du RPi, lancer avec :
#   docker run --net=host --privileged \
#     -e MQTT_BROKER=<ip_vps> -e INFLUX_TOKEN=<token> \
#     murmetric-rpi
#
# L'ingestion DeweSoftX (PC Windows) est gérée séparément
# par start_dewesoft.py sur le PC labo.

WORKDIR /app

COPY requirements-rpi.txt ./
RUN pip install --no-cache-dir -r requirements-rpi.txt

COPY capteurs.json ./
COPY configure_capteurs.py ./
COPY ingestion_capteurs_bluetooth.py ./
COPY bridge_mqtt_to_influx.py ./
COPY start.py ./

CMD ["python", "-u", "start.py"]