# Certificats TLS — Mosquitto (production uniquement)

Ce dossier doit contenir, avant d'utiliser `docker-compose.prod.yml` :

- `fullchain.pem`
- `privkey.pem`

Ces deux fichiers sont **gitignorés** (jamais commités). Deux méthodes
d'obtention selon qu'un nom de domaine est disponible ou non :

**Avec nom de domaine** — Let's Encrypt (nécessite une validation de
domaine, impossible pour une IP nue) :

```
certbot certonly --standalone -d mqtt.votredomaine.exemple
cp /etc/letsencrypt/live/mqtt.votredomaine.exemple/fullchain.pem ./certs/
cp /etc/letsencrypt/live/mqtt.votredomaine.exemple/privkey.pem  ./certs/
```

**Sans nom de domaine** (cas du VPS actuel, cf. section 28 de
`logique_projet.md`) — certificat auto-signé, SAN = IP publique du VPS :

```
openssl req -x509 -nodes -newkey rsa:2048 -keyout ./certs/privkey.pem \
  -out ./certs/fullchain.pem -days 3650 \
  -subj '/CN=<IP_VPS>/O=MurMetric-FRD-CODEM' \
  -addext 'subjectAltName=IP:<IP_VPS>'
```

Le SAN doit être l'IP littérale, pas un nom : les clients valident le nom du
serveur (celui passé à `connect()`) contre ce SAN pendant le handshake TLS.
Les clients doivent alors faire explicitement confiance à ce certificat (pas
de chaîne de confiance publique) — cf. `MQTT_CA_CERT` dans les scripts
d'ingestion.

Renouvellement (certbot renew, en cron) suivi d'un redémarrage de mosquitto :

```
docker compose -f docker-compose.yml -f docker-compose.prod.yml restart mosquitto
```

Sans ces deux fichiers, `docker-compose.prod.yml` ne démarrera pas (mosquitto
refuse de démarrer un listener TLS sans certificat valide).
