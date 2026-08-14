#!/bin/bash
# Mesure périodique de l'espace disque utilisé par InfluxDB — écrit un point
# dans InfluxDB lui-même (mesure disk_usage_bytes, tag host=influxdb-0),
# lu ensuite par la webapp (GET /api/monitoring/espace-disque) pour un
# suivi de l'évolution dans le temps (section 32, 14/08/2026).
#
# Installé en tâche cron sur le VPS (crontab de l'utilisateur `ubuntu`),
# PAS en CronJob Kubernetes : la webapp tourne dans un pod séparé de celui
# d'InfluxDB, sans accès à son système de fichiers, et lui donner cet accès
# (exec dans un autre pod) demanderait des permissions RBAC plus larges que
# nécessaire. kubectl exec depuis l'hôte (déjà utilisé manuellement tout au
# long du projet pour vérifier cette même taille) reste la solution la plus
# simple, sans nouveau composant ni permission.
#
# Installation (déjà faite le 14/08/2026, conservé ici pour référence/
# redéploiement) :
#   crontab -e
#   0 */6 * * * /home/ubuntu/Projets_en_Production/murmetric/scripts/verifier_espace_disque_influxdb.sh >> /home/ubuntu/Projets_en_Production/murmetric/scripts/espace_disque.log 2>&1
set -euo pipefail

NAMESPACE=murmetric
POD=influxdb-0
BUCKET=Test_Capteurs
ORG=FRD_CODEM
CHEMIN_DONNEES=/var/lib/influxdb2

TOKEN=$(kubectl get secret murmetric-secrets -n "$NAMESPACE" -o jsonpath='{.data.influx-token}' | base64 -d)
OCTETS=$(kubectl exec -n "$NAMESPACE" "$POD" -- du -sb "$CHEMIN_DONNEES" | cut -f1)

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) espace disque InfluxDB : ${OCTETS} octets"

kubectl exec -n "$NAMESPACE" "$POD" -- influx write \
  --org "$ORG" --token "$TOKEN" --bucket "$BUCKET" \
  "disk_usage_bytes,host=influxdb-0 value=${OCTETS}i"
