"""
Consommateur Kafka → InfluxDB — VPS cloud, MurMetric / FRD-CODEM.

Consomme les topics Kafka MurMetric et écrit les points dans InfluxDB
en mode batch asynchrone, adapté au débit élevé des capteurs de retrait
DeweSoftX (1 mesure/s par canal).

Topics consommés (namespaced par tenant) :
    murmetric.{tenant}.capteurs.bruts    → mesure : mesures_capteurs
    murmetric.{tenant}.capteurs.registre → mesure : registre_capteurs
    murmetric.{tenant}.dewesoft.bruts    → mesure : mesures_dewesoft
    murmetric.{tenant}.dewesoft.alertes  → mesure : alertes_ingestion

Mode d'écriture InfluxDB : SYNCHRONE, avec validation Kafka manuelle
    (08/08/2026 — cf. logique_projet.md).

    Le mode batch ASYNCHRONE précédent perdait des points en silence. Trace
    complète du 07/08/2026 sur un import réel : le point HA2 du
    2025-11-21T14:14:19.447996Z était présent et bien formé dans Kafka (lot de
    600 valeurs, k=305, valeur -0.01744562122183524), le lag du consumer était
    nul, aucun rappel d'erreur ni de ré-essai ne s'est déclenché, InfluxDB n'a
    journalisé aucun rejet — et le point n'existe pas dans le bucket. Une
    seule ligne manquante au milieu d'un lot par ailleurs intégralement écrit :
    ce n'est donc PAS la fenêtre « au plus une fois » de la validation
    automatique des offsets (qui perdrait les 600 points du message), mais un
    défaut du writer asynchrone lui-même.

    Plutôt que de continuer à diagnostiquer ce writer, on rend la perte
    structurellement impossible — ou à défaut BRUYANTE :

      1. Écriture SYNCHRONE : write() ne rend la main qu'une fois InfluxDB
         ayant accusé réception, et lève une exception en cas d'échec. Plus
         aucun lot ne peut disparaître dans une file d'arrière-plan.
      2. enable_auto_commit=False : l'offset Kafka n'est JAMAIS validé avant
         que l'écriture correspondante ne soit confirmée. Un incident rejoue
         les messages non validés au lieu de les perdre.

    Garantie obtenue : « au moins une fois ». Les doublons éventuels d'un
    rejeu sont inoffensifs car l'écriture est IDEMPOTENTE — un point .dxd est
    identifié par (mesure, tags, horodatage) et son horodatage est reconstitué
    de façon déterministe depuis le fichier source (t0_ns + k*dt_ns) : le
    réécrire produit exactement le même point.

    Coût : un aller-retour HTTP synchrone vers InfluxDB par message consommé.
    Compromis assumé — la correction des données prime sur le débit.

Pourquoi un consommateur séparé du bridge ?
    - Indépendance : un futur consommateur "alertes" peut lire les mêmes
      topics Kafka sans modifier ce script.
    - Reprise sur panne : si InfluxDB redémarre, le consumer group Kafka
      reprend depuis le dernier offset committé, sans perte de données.
    - Scalabilité : plusieurs instances de ce script peuvent tourner en
      parallèle (même group_id) pour répartir la charge.

Usage :
    python kafka_consumer_influx.py

    Variables d'environnement :
        KAFKA_BOOTSTRAP   Serveurs Kafka             (défaut : localhost:9092)
        KAFKA_GROUP_ID    Consumer group ID          (défaut : murmetric-influx)
        TENANT_ID         Identifiant tenant         (défaut : frd)
        INFLUX_URL        URL InfluxDB               (défaut : http://localhost:8086)
        INFLUX_TOKEN      Token API InfluxDB
        INFLUX_ORG        Organisation InfluxDB      (défaut : FRD_CODEM)
        INFLUX_BUCKET     Bucket de destination      (défaut : Capteurs)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer

sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Configuration Kafka.
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "murmetric-influx")
TENANT_ID = os.getenv("TENANT_ID", "frd")

# ---------------------------------------------------------------------------
# Configuration InfluxDB.
# ---------------------------------------------------------------------------
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "MON_TOKEN_API_GENERE_PAR_INFLUXDB")
INFLUX_ORG = os.getenv("INFLUX_ORG", "FRD_CODEM")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Capteurs")

# Période du récapitulatif d'écriture, en secondes (cf. boucle de consommation).
LOG_INTERVAL = float(os.getenv("LOG_INTERVAL", "10"))

# Noms des mesures InfluxDB.
MESURE_CAPTEURS = "mesures_capteurs"
MESURE_REGISTRE = "registre_capteurs"
MESURE_DEWESOFT = "mesures_dewesoft"
MESURE_ALERTES = "alertes_ingestion"

# Topics Kafka à consommer (namespaced par tenant).
TOPICS = [
    f"murmetric.{TENANT_ID}.capteurs.bruts",
    f"murmetric.{TENANT_ID}.capteurs.registre",
    f"murmetric.{TENANT_ID}.dewesoft.bruts",
    f"murmetric.{TENANT_ID}.dewesoft.alertes",
]


# ---------------------------------------------------------------------------
# Constructeurs de points InfluxDB — écriture directe en line protocol.
#
# Le SDK officiel (classe Point, .tag()/.field() chaînés) s'est révélé être
# le facteur limitant du débit d'écriture lors de l'import massif de fichiers
# .dxd historiques (mesuré : ~372m CPU pour ce process contre 203m pour le
# bridge MQTT→Kafka, qui traite le même volume de messages). Construire la
# ligne "measurement,tags fields timestamp" nous-mêmes, par simple
# concaténation de chaînes, évite tout le travail interne du SDK (validation
# de types, copies d'objets) pour un format qu'on connaît déjà entièrement à
# l'écriture. write_api.write() accepte une chaîne de line protocol brute
# aussi bien qu'un objet Point — cf. logique_projet.md, 07/08/2026.
# ---------------------------------------------------------------------------

def _echap_tag(valeur: str) -> str:
    """Échapper une clé/valeur de tag pour le line protocol (virgule, égal, espace)."""
    return valeur.replace("\\", "\\\\").replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def _echap_field_str(valeur: str) -> str:
    """Échapper une valeur de field string pour le line protocol (guillemet, antislash)."""
    return valeur.replace("\\", "\\\\").replace('"', '\\"')


def construire_point_capteurs(data: dict) -> str | None:
    """Construire un point InfluxDB pour les mesures BLE (T°/HR%).

    Args:
        data: Payload JSON du topic ``murmetric.*.capteurs.bruts``.

    Returns:
        Ligne InfluxDB (line protocol) ou None si les mesures principales
        sont invalides.
    """
    temperature = data.get("temperature_c")
    humidite = data.get("humidite_percent")

    if not isinstance(temperature, (int, float)) or not isinstance(
        humidite, (int, float)
    ):
        return None

    # Tags dans l'ordre alphabétique — le SDK Point() les triait ainsi ;
    # écrire l'ordre en dur ici ne coûte rien à l'exécution (pas de tri
    # réel) et évite au serveur InfluxDB ce travail à l'écriture.
    tags = (
        f"adresse_mac={_echap_tag(str(data.get('mac', 'inconnu')))},"
        f"emplacement={_echap_tag(str(data.get('emplacement') or 'Non défini'))},"
        f"nom_capteur={_echap_tag(str(data.get('capteur_id', 'Inconnu')))}"
    )
    fields = f"temperature={float(temperature)},humidite={float(humidite)}"

    point_de_rosee = data.get("point_de_rosee_c")
    if isinstance(point_de_rosee, (int, float)):
        fields += f",point_de_rosee={float(point_de_rosee)}"

    batterie = data.get("batterie_percent")
    if isinstance(batterie, (int, float)):
        fields += f",batterie={int(batterie)}i"

    rssi = data.get("rssi_dbm")
    if isinstance(rssi, (int, float)):
        fields += f",rssi={int(rssi)}i"

    return f"{MESURE_CAPTEURS},{tags} {fields}"


def construire_point_registre(data: dict) -> str | None:
    """Construire un point InfluxDB pour le registre de configuration capteurs.

    Args:
        data: Payload JSON du topic ``murmetric.*.capteurs.registre``.

    Returns:
        Ligne InfluxDB (line protocol) ou None si la MAC est absente.
    """
    mac = data.get("mac", "").strip()
    if not mac:
        return None

    tags = (
        f"adresse_mac={_echap_tag(mac)},"
        f"emplacement={_echap_tag(str(data.get('emplacement') or 'Non défini'))},"
        f"nom={_echap_tag(str(data.get('nom') or 'Non défini'))},"
        f"nom_couche={_echap_tag(str(data.get('nom_couche') or 'Non défini'))},"
        f"nom_mur={_echap_tag(str(data.get('nom_mur') or 'Non défini'))},"
        f"position={_echap_tag(str(data.get('position') or 'Non défini'))},"
        f"prestation={_echap_tag(str(data.get('prestation') or 'Non défini'))},"
        f"rd={_echap_tag(str(data.get('categorie R&D') or 'Non défini'))}"
    )
    ingestion = "true" if data.get("ingestion", False) else "false"
    lint_configure = "true" if data.get("lint_configure", False) else "false"
    fields = f"ingestion={ingestion},lint_configure={lint_configure}"

    lint_max = data.get("lint_max_confirme_s")
    if isinstance(lint_max, (int, float)):
        fields += f",lint_max_confirme_s={float(lint_max)}"

    return f"{MESURE_REGISTRE},{tags} {fields}"


def construire_point_dewesoft(data: dict) -> str | None:
    """Construire un point InfluxDB pour les mesures DeweSoftX (retrait).

    Structure du payload (depuis ingestion_dewesoft_dxd.py, import .dxd) :
        source, canal_index, canal_nom, canal_unite, valeur,
        horodatage_dewesoft, horodatage, taux_echantillonnage,
        horodatage_mesure_iso — horodatage réel de la mesure (reconstitué à
        partir du début d'enregistrement du fichier .dxd), utilisé pour dater
        le point (_time) plutôt que l'instant d'écriture Kafka/InfluxDB, afin
        de ne pas écraser l'historique lors d'un import différé. Republié en
        plus comme field "horodatage_lisible" (JJ/MM/AAAA HH:MM:SS) pour
        lecture directe dans un export brut, sans reparser _time.

        valeur_filtree / est_aberrant — sortie du filtre de Hampel anti-
        vibration appliqué à l'ingestion (cf. filtrer_hampel() dans
        ingestion_dewesoft_dxd.py), avec un seuil par défaut. La valeur brute
        (field "valeur") n'est jamais modifiée : les deux sont conservées
        pour permettre une visualisation brut/filtré côte à côte côté appli.

    Args:
        data: Payload JSON du topic ``murmetric.*.dewesoft.bruts``.

    Returns:
        Ligne InfluxDB (line protocol) ou None si la valeur de mesure est
        invalide.
    """
    valeur = data.get("valeur")
    if not isinstance(valeur, (int, float)):
        return None

    tags = (
        f"canal_nom={_echap_tag(str(data.get('canal_nom', 'inconnu')))},"
        f"canal_unite={_echap_tag(str(data.get('canal_unite', '')))},"
        f"nom_couche={_echap_tag(str(data.get('nom_couche') or 'Non défini'))},"
        f"nom_mur={_echap_tag(str(data.get('nom_mur') or 'Non défini'))},"
        f"position={_echap_tag(str(data.get('position') or 'Non défini'))},"
        f"rd={_echap_tag(str(data.get('categorie R&D') or 'Non défini'))},"
        f"source={_echap_tag(str(data.get('source', 'dewesoft')))}"
    )
    fields = f"valeur={float(valeur)},canal_index={int(data.get('canal_index', 0))}i"

    taux = data.get("taux_echantillonnage")
    if isinstance(taux, (int, float)):
        fields += f",taux_echantillonnage={float(taux)}"

    valeur_filtree = data.get("valeur_filtree")
    if isinstance(valeur_filtree, (int, float)):
        fields += f",valeur_filtree={float(valeur_filtree)}"

    est_aberrant = data.get("est_aberrant")
    if isinstance(est_aberrant, bool):
        fields += f",est_aberrant={'true' if est_aberrant else 'false'}"

    # Import .dxd : dater le point avec l'horodatage réel de la mesure plutôt
    # que l'instant d'écriture — sinon un import différé écraserait tout
    # l'historique sur la date d'exécution du consumer.
    horodatage_mesure_iso = data.get("horodatage_mesure_iso")
    timestamp_ns = ""
    horodatage_mesure_iso_str = horodatage_mesure_iso if isinstance(horodatage_mesure_iso, str) else None
    if horodatage_mesure_iso_str:
        try:
            horodatage_mesure = datetime.fromisoformat(horodatage_mesure_iso_str)
            # datetime.timestamp() interprète un datetime naïf (sans fuseau)
            # dans le fuseau LOCAL de la machine qui exécute ce script — donc
            # un résultat différent selon où tourne le consumer. En pratique
            # ingestion_dewesoft_dxd.py envoie toujours un horodatage avec
            # fuseau (UTC), mais on fixe explicitement UTC si jamais un
            # producteur futur envoie une chaîne naïve, pour un comportement
            # déterministe indépendant de la machine.
            if horodatage_mesure.tzinfo is None:
                horodatage_mesure = horodatage_mesure.replace(tzinfo=timezone.utc)
            fields += (
                f',horodatage_lisible="{horodatage_mesure.strftime("%d/%m/%Y %H:%M:%S")}"'
            )
            timestamp_ns = f" {int(horodatage_mesure.timestamp() * 1_000_000) * 1_000}"
        except ValueError:
            pass

    return f"{MESURE_DEWESOFT},{tags} {fields}{timestamp_ns}"


def construire_points_dewesoft_lot(data: dict) -> list[str]:
    """Déplier un LOT d'échantillons DeweSoft en N lignes de line protocol.

    Format produit par ingestion_dewesoft_dxd.py depuis le 07/08/2026 (cf.
    DXD_BATCH_SIZE) : un seul message porte les métadonnées communes du canal
    (fichier, mur, couche, position, unité, fréquence) plus les tableaux
    ``valeurs`` / ``valeurs_filtrees`` et la grille d'horodatage ``t0_ns`` /
    ``dt_ns``. L'échantillon k est daté t0_ns + k*dt_ns — arithmétique
    entière en nanosecondes, donc sans dérive flottante sur la durée du lot.

    Les tags sont identiques pour tous les points du lot : la portion
    ``mesure,tags`` de la ligne est donc construite UNE fois puis réutilisée,
    ce qui est l'essentiel du gain CPU côté consumer.

    Args:
        data: Payload JSON d'un message de lot du topic ``*.dewesoft.bruts``.

    Returns:
        Liste de lignes InfluxDB (line protocol), éventuellement vide.
    """
    valeurs = data.get("valeurs")
    if not isinstance(valeurs, list) or not valeurs:
        return []

    t0_ns = data.get("t0_ns")
    dt_ns = data.get("dt_ns", 0)
    if not isinstance(t0_ns, int) or not isinstance(dt_ns, int):
        return []

    prefixe = (
        f"{MESURE_DEWESOFT},"
        f"canal_nom={_echap_tag(str(data.get('canal_nom', 'inconnu')))},"
        f"canal_unite={_echap_tag(str(data.get('canal_unite', '')))},"
        f"nom_couche={_echap_tag(str(data.get('nom_couche') or 'Non défini'))},"
        f"nom_mur={_echap_tag(str(data.get('nom_mur') or 'Non défini'))},"
        f"position={_echap_tag(str(data.get('position') or 'Non défini'))},"
        f"rd={_echap_tag(str(data.get('categorie R&D') or 'Non défini'))},"
        f"source={_echap_tag(str(data.get('source', 'dewesoft')))}"
    )

    canal_index = int(data.get("canal_index", 0))
    taux = data.get("taux_echantillonnage")
    suffixe_taux = (
        f",taux_echantillonnage={float(taux)}" if isinstance(taux, (int, float)) else ""
    )

    filtrees = data.get("valeurs_filtrees")
    if not isinstance(filtrees, list) or len(filtrees) != len(valeurs):
        filtrees = None
    aberrants = data.get("indices_aberrants")
    aberrants = set(aberrants) if isinstance(aberrants, list) else set()

    lignes = []
    for k, valeur in enumerate(valeurs):
        if not isinstance(valeur, (int, float)):
            continue
        ts_ns = t0_ns + k * dt_ns
        # strftime attend un datetime : reconstruit par point, comme le
        # faisait l'ancien format à partir de sa chaîne ISO.
        lisible = datetime.fromtimestamp(ts_ns / 1_000_000_000, timezone.utc).strftime(
            "%d/%m/%Y %H:%M:%S"
        )
        fields = f"valeur={float(valeur)},canal_index={canal_index}i{suffixe_taux}"
        if filtrees is not None and isinstance(filtrees[k], (int, float)):
            fields += f",valeur_filtree={float(filtrees[k])}"
        fields += f",est_aberrant={'true' if k in aberrants else 'false'}"
        fields += f',horodatage_lisible="{lisible}"'
        lignes.append(f"{prefixe} {fields} {ts_ns}")

    return lignes


def construire_point_alerte(data: dict) -> str | None:
    """Construire un point InfluxDB pour une anomalie d'ingestion.

    Aucune journalisation persistante n'existe ailleurs dans le pipeline
    (tout part sur stdout, perdu si personne ne regarde) — publier chaque
    anomalie comme point InfluxDB la rend persistante, horodatée,
    interrogeable en Flux et visible dans Grafana sans outil supplémentaire
    (cf. logique_projet.md section 19). Premier cas d'usage : collision de
    nom de canal DeweSoft (deux canaux de même nom dans un même fichier .dxd).

    Args:
        data: Payload JSON du topic ``murmetric.*.dewesoft.alertes``.

    Returns:
        Ligne InfluxDB (line protocol), ou None si le type d'anomalie est
        absent.
    """
    type_alerte = data.get("type")
    if not type_alerte:
        return None

    tags = (
        f"canal_nom={_echap_tag(str(data.get('canal_nom', 'inconnu')))},"
        f"fichier_source={_echap_tag(str(data.get('fichier_source', 'inconnu')))},"
        f"type={_echap_tag(str(type_alerte))}"
    )
    fields = f"occurrences={int(data.get('occurrences', 0))}i"

    return f"{MESURE_ALERTES},{tags} {fields}"


# ---------------------------------------------------------------------------
# Router : topic Kafka → constructeur de point InfluxDB.
# ---------------------------------------------------------------------------

def construire_points_dewesoft(data: dict) -> list[str]:
    """Router un message DeweSoft vers le constructeur de son format.

    Deux formats coexistent volontairement sur ce topic :
      - LOT (``valeurs`` est une liste, format 2, depuis le 07/08/2026) ;
      - point par point (ancien format, un échantillon par message).
    Le second reste géré pour que les messages déjà en vol dans Kafka ou
    rejoués depuis le buffer SQLite d'Amiens au moment de la bascule soient
    toujours écrits correctement — la détection se fait sur la charge utile,
    pas sur une configuration, donc aucun ordre de déploiement n'est imposé.
    """
    if isinstance(data.get("valeurs"), list):
        return construire_points_dewesoft_lot(data)
    ligne = construire_point_dewesoft(data)
    return [ligne] if ligne else []


def _un_point(constructeur):
    """Adapter un constructeur mono-point à l'interface « liste de lignes »."""
    def adapte(data: dict) -> list[str]:
        ligne = constructeur(data)
        return [ligne] if ligne else []
    return adapte


CONSTRUCTEURS = {
    f"murmetric.{TENANT_ID}.capteurs.bruts":    _un_point(construire_point_capteurs),
    f"murmetric.{TENANT_ID}.capteurs.registre": _un_point(construire_point_registre),
    f"murmetric.{TENANT_ID}.dewesoft.bruts":    construire_points_dewesoft,
    f"murmetric.{TENANT_ID}.dewesoft.alertes":  _un_point(construire_point_alerte),
}

# ---------------------------------------------------------------------------
# Initialisation InfluxDB — mode batch asynchrone.
# ---------------------------------------------------------------------------

print("⏳ Connexion à InfluxDB...")
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

# Écriture SYNCHRONE : write() lève une exception si InfluxDB n'a pas accusé
# réception. Aucun rappel d'erreur n'est nécessaire — il n'y a plus de file
# d'arrière-plan dans laquelle un lot pourrait disparaître (cf. docstring).
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# Nombre de tentatives d'écriture avant d'abandonner le processus. On ne saute
# JAMAIS un lot : si InfluxDB reste injoignable, on sort en erreur sans avoir
# validé l'offset, et Kafka rejouera les messages au redémarrage du pod.
ECRITURE_MAX_TENTATIVES = int(os.getenv("ECRITURE_MAX_TENTATIVES", "5"))
ECRITURE_DELAI_BASE = float(os.getenv("ECRITURE_DELAI_BASE", "2"))

_nb_reessais_ecriture = 0


def ecrire_ou_mourir(lignes: list[str]) -> None:
    """Écrire un lot dans InfluxDB, en réessayant, sans jamais l'abandonner.

    Contrat : soit la fonction rend la main et les points SONT écrits, soit
    elle lève — et l'appelant ne doit alors pas valider l'offset Kafka. C'est
    exactement ce qui rend la perte silencieuse impossible.
    """
    global _nb_reessais_ecriture
    for tentative in range(1, ECRITURE_MAX_TENTATIVES + 1):
        try:
            write_api.write(
                bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=lignes,
                write_precision=WritePrecision.NS,
            )
            return
        except Exception as exc:
            _nb_reessais_ecriture += 1
            apercu = lignes[0][:180] if lignes else ""
            print(
                f"⚠️  Échec d'écriture InfluxDB (tentative "
                f"{tentative}/{ECRITURE_MAX_TENTATIVES}) : {exc} — "
                f"{len(lignes)} point(s), début du lot : {apercu!r}",
                flush=True,
            )
            if tentative == ECRITURE_MAX_TENTATIVES:
                print(
                    "❌ ABANDON : InfluxDB refuse toujours l'écriture. Arrêt du "
                    "consumer SANS validation de l'offset Kafka — les messages "
                    "seront rejoués au redémarrage, aucun point n'est perdu.",
                    flush=True,
                )
                raise
            time.sleep(ECRITURE_DELAI_BASE * (2 ** (tentative - 1)))


print(f"✅ InfluxDB prêt ({INFLUX_URL}) — écriture SYNCHRONE")

# ---------------------------------------------------------------------------
# Initialisation consommateur Kafka.
# ---------------------------------------------------------------------------

def _connecter_consommateur_kafka() -> KafkaConsumer:
    """Créer le consumer Kafka, en réessayant tant que le bootstrap échoue.

    KafkaConsumer() se connecte au cluster dès sa construction et lève une
    exception immédiate si Kafka est injoignable — sans ce ré-essai, le
    conteneur planterait au moindre léger décalage de démarrage entre
    services (ex. Kubernetes, où l'ordre de disponibilité des pods n'est pas
    garanti malgré depends_on/initContainers).
    """
    while True:
        try:
            return KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP_ID,
                # earliest : reprendre depuis le début si ce consumer group est nouveau.
                # Garantit qu'aucun message historique Kafka n'est perdu au démarrage.
                auto_offset_reset="earliest",
                # Validation MANUELLE : un offset n'est validé qu'après
                # confirmation de l'écriture InfluxDB correspondante (cf.
                # docstring). Avec enable_auto_commit=True, un offset pouvait
                # être validé avant que ses points ne soient réellement
                # écrits — les messages n'étaient alors jamais rejoués.
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
        except Exception as exc:
            print(f"⚠️  Kafka injoignable ({exc}) — nouvelle tentative dans 5 s...")
            time.sleep(5)


print(f"⏳ Connexion à Kafka ({KAFKA_BOOTSTRAP})...")
consommateur = _connecter_consommateur_kafka()
print(f"✅ Kafka prêt — group_id : {KAFKA_GROUP_ID}")
print(f"   Topics : {', '.join(TOPICS)}\n")

# ---------------------------------------------------------------------------
# Boucle de consommation.
# ---------------------------------------------------------------------------

nb_ecrits = 0
nb_au_dernier_log = 0
dernier_log = time.time()

# Cadence de validation des offsets Kafka (cf. boucle ci-dessous). Un commit
# tous les 200 messages ou toutes les 5 s : assez fréquent pour qu'un incident
# ne rejoue qu'un volume modeste, assez rare pour ne pas doubler le coût
# réseau de chaque lot.
COMMIT_TOUS_LES_N = int(os.getenv("COMMIT_TOUS_LES_N", "200"))
COMMIT_INTERVAL = float(os.getenv("COMMIT_INTERVAL", "5"))
messages_non_valides = 0
dernier_commit = time.time()

try:
    for message in consommateur:
        topic = message.topic
        data = message.value

        constructeur = CONSTRUCTEURS.get(topic)
        if constructeur is None:
            continue

        lignes = constructeur(data)
        if not lignes:
            continue

        # Écriture SYNCHRONE : au retour, les points SONT dans InfluxDB. Toute
        # défaillance lève (après ré-essais) au lieu de disparaître en
        # silence — c'est le cœur du correctif du 08/08/2026.
        #
        # Liste (et non chaîne jointe par "\n") : chaque LIGNE est un
        # enregistrement distinct pour le SDK.
        ecrire_ou_mourir(lignes)

        # Compte des POINTS écrits, pas des messages Kafka : un message de lot
        # en porte jusqu'à DXD_BATCH_SIZE, sinon le débit affiché serait
        # divisé par la taille du lot.
        nb_ecrits += len(lignes)
        messages_non_valides += 1

        maintenant = time.time()

        # Validation Kafka APRÈS écriture confirmée, et seulement
        # périodiquement : un commit() par message coûterait un aller-retour
        # réseau supplémentaire à chaque lot. Retarder la validation n'expose
        # qu'à un REJEU (messages déjà écrits rejoués après un incident), sans
        # danger puisque l'écriture est idempotente — alors que l'anticiper
        # exposerait à une PERTE, ce qu'on refuse.
        if (messages_non_valides >= COMMIT_TOUS_LES_N
                or maintenant - dernier_commit >= COMMIT_INTERVAL):
            consommateur.commit()
            messages_non_valides = 0
            dernier_commit = maintenant

        # Récapitulatif périodique — surtout PAS un print par message : les
        # images tournent avec "python -u", donc chaque print est un appel
        # système write() relayé sur disque par containerd. Lors d'un import
        # massif de .dxd historiques (10 Hz × 8 canaux/fichier), cela
        # bloquait la boucle de consommation Kafka. Même correction que dans
        # bridge_mqtt_to_kafka.py (cf. logique_projet.md, 06/08/2026).
        if maintenant - dernier_log >= LOG_INTERVAL:
            delta = nb_ecrits - nb_au_dernier_log
            print(
                f"💾 {delta} point(s) écrit(s) en {maintenant - dernier_log:.0f}s "
                f"({delta / (maintenant - dernier_log):.0f}/s) — cumul {nb_ecrits}"
                + (f" — {_nb_reessais_ecriture} ré-essai(s) d'écriture"
                   if _nb_reessais_ecriture else ""),
                flush=True,
            )
            dernier_log = maintenant
            nb_au_dernier_log = nb_ecrits

except KeyboardInterrupt:
    print("\n🛑 Arrêt demandé.")
    # Arrêt propre : tout ce qui est écrit l'est réellement (mode synchrone),
    # on peut donc valider les offsets correspondants sans risque.
    try:
        consommateur.commit()
        print("✅ Offsets Kafka validés avant l'arrêt.")
    except Exception as exc:
        print(f"⚠️  Validation finale des offsets impossible ({exc}) — "
              "les derniers messages seront rejoués (sans perte).")
finally:
    # Surtout PAS de commit() ici : ce bloc s'exécute aussi quand
    # ecrire_ou_mourir() a levé. Valider à cet instant reviendrait à déclarer
    # écrits des points qui ne le sont pas — précisément la perte qu'on
    # cherche à rendre impossible. Sans validation, Kafka les rejouera.
    write_api.close()
    influx_client.close()
    consommateur.close()
    print(f"👋 Consommateur Kafka → InfluxDB arrêté. "
          f"{nb_ecrits} point(s) écrit(s), "
          f"{_nb_reessais_ecriture} ré-essai(s) d'écriture.")
