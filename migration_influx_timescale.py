"""Migration InfluxDB -> TimescaleDB (Phase 1, chantier section 33,
17/08/2026) — lecture SEULE sur InfluxDB, aucune écriture ni suppression
côté source à aucun moment. Migre mesure par mesure, en lots bornés (une
valeur de tag de découpage + un jour), avec suivi de progression et
vérification systématique du comptage avant de marquer un lot comme migré
et vérifié — un lot non vérifié est retenté au prochain lancement, jamais
ignoré silencieusement.

RÈGLE ABSOLUE (cf. incidents OOM du 17/08/2026, section 33 — DEUX incidents
distincts) : aucune requête InfluxDB sur mesures_dewesoft (1,5 milliard de
points) sans borne temporelle stricte ET filtre sur une seule valeur de
canal à la fois — jamais de requête combinant plusieurs canaux. ET :
toutes les requêtes passent par `kubectl exec ... influx query --raw`
(sous-processus), JAMAIS par la bibliothèque Python `influxdb-client` en
réseau direct depuis l'hôte VPS vers le ClusterIP — cette dernière a
déclenché deux OOM-kill du pod InfluxDB le 17/08/2026 avec des requêtes
pourtant déjà validées sûres (1-3s) via le chemin kubectl exec. Cause
exacte non identifiée (probablement une différence de négociation HTTP
entre le client Python et le CLI Go), mais le chemin kubectl exec est
systématiquement rapide et sûr sur ce projet — on s'y tient.

Idempotence : chaque lot est migré par DELETE (sur la fenêtre exacte
tag+jour) puis INSERT — sûr de relancer un lot déjà migré (résultat
identique), pas besoin de contrainte UNIQUE coûteuse à maintenir pendant
le chargement en masse.

Usage (à exécuter directement sur le VPS, kubectl/sudo déjà configurés) :
    python migration_influx_timescale.py --mesure mesures_dewesoft --canal HA1
    python migration_influx_timescale.py --mesure mesures_dewesoft
    python migration_influx_timescale.py --toutes

Variables d'environnement : INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET,
INFLUX_NAMESPACE, INFLUX_POD (mêmes défauts que le déploiement k8s actuel),
TIMESCALE_HOST, TIMESCALE_PORT, TIMESCALE_DB, TIMESCALE_USER,
TIMESCALE_PASSWORD.
"""

import argparse
import csv
import io
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone

import psycopg2

INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "FRD_CODEM")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Test_Capteurs")
INFLUX_NAMESPACE = os.getenv("INFLUX_NAMESPACE", "murmetric")
INFLUX_POD = os.getenv("INFLUX_POD", "influxdb-0")

TIMESCALE_HOST = os.getenv("TIMESCALE_HOST", "localhost")
TIMESCALE_PORT = os.getenv("TIMESCALE_PORT", "5432")
TIMESCALE_DB = os.getenv("TIMESCALE_DB", "murmetric_timeseries")
TIMESCALE_USER = os.getenv("TIMESCALE_USER", "murmetric")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "")

sys.stdout.reconfigure(encoding="utf-8")

# Borne de départ pour toute requête qui a besoin de "toute l'historique" —
# JAMAIS range(start: 0) (epoch), même avec un filtre tag+champ qui donne
# une série unique (cf. incidents du 17/08/2026, section 33 : range(start:0)
# empêche l'élagage par shard côté InfluxDB, forçant potentiellement un scan
# de tous les shards — observé : requête passant de 1s à >20s et 4Gi de
# mémoire selon l'état du moteur). Bien antérieure à toute donnée connue du
# projet (campagne démarrée courant 2025) : marge large sans reproduire le
# problème d'une borne non bornée.
DATE_DEBUT_HISTORIQUE = "2024-01-01T00:00:00Z"

# ---------------------------------------------------------------------------
# Configuration par mesure.
#
# tag_decoupage : un lot = une valeur de ce tag + un jour. None pour les
# mesures à faible volume, migrées d'un bloc sans découpage par tag.
# tags_constants : tags fetchés une fois par valeur de découpage (supposés
# stables sur toute l'historique d'un canal/capteur — vérifié en pratique
# le 17/08/2026 : mêmes valeurs sur le premier ET le dernier point de HA1).
# ---------------------------------------------------------------------------
MESURES = {
    "mesures_dewesoft": {
        "tag_decoupage": "canal_nom",
        "tags_constants": ["canal_unite", "nom_mur", "nom_couche", "position", "rd", "source"],
        "champs": [
            "valeur",
            "valeur_filtree",
            "est_aberrant",
            "canal_index",
            "taux_echantillonnage",
            "horodatage_lisible",
        ],
    },
    "mesures_capteurs": {
        "tag_decoupage": "adresse_mac",
        "tags_constants": ["emplacement", "nom_capteur", "nom_mur", "nom_couche", "position", "rd"],
        "champs": ["temperature", "humidite", "point_de_rosee", "mac_complete_connue"],
    },
    "mesures_teneur_eau": {
        "tag_decoupage": None,
        "tags_constants": [],
        "tags_par_point": ["mur", "couche", "prestation", "utilisateur_id", "utilisateur_nom"],
        "champs": ["teneur_eau_pourcent", "commentaire"],
    },
    "pipeline_heartbeat": {
        "tag_decoupage": None,
        "tags_constants": [],
        "tags_par_point": ["pipeline", "machine"],
        "champs": [
            "mqtt_connecte",
            "buffer_sqlite_en_attente",
            "registre_api_ok",
            "nb_capteurs_connus",
            "demarre_le",
            "nb_points_publies",
            "nb_points_bufferises",
        ],
    },
    "disk_usage_bytes": {
        "tag_decoupage": None,
        "tags_constants": [],
        "tags_par_point": ["host"],
        "champs": ["value"],
    },
}


def _parser_csv_annote(texte: str) -> list[dict]:
    """Parse le format CSV annoté de `influx query --raw` — lignes `#...`
    ignorées, une ligne vide referme un bloc (l'en-tête suivant redéfinit les
    colonnes), plusieurs blocs (plusieurs tables Flux) sont concaténés."""
    lignes = []
    entetes = None
    for ligne in csv.reader(io.StringIO(texte)):
        if not ligne:
            entetes = None
            continue
        if ligne[0].startswith("#"):
            continue
        if entetes is None:
            entetes = ligne
            continue
        lignes.append(dict(zip(entetes, ligne)))
    return lignes


def flux_query(flux: str, timeout: int = 20) -> list[dict]:
    """Exécute une requête Flux via `kubectl exec ... influx query --raw` — SEUL
    chemin validé sans risque sur ce projet (cf. docstring de tête de fichier).
    `timeout` en secondes : filet de sécurité, jamais censé se déclencher sur
    une requête correctement bornée (toutes les requêtes de ce script le
    sont), mais on ne bloque jamais indéfiniment si un cas imprévu survient."""
    resultat = subprocess.run(
        [
            "sudo",
            "kubectl",
            "exec",
            INFLUX_POD,
            "-n",
            INFLUX_NAMESPACE,
            "--",
            "influx",
            "query",
            flux,
            "-o",
            INFLUX_ORG,
            "-t",
            INFLUX_TOKEN,
            "--raw",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if resultat.returncode != 0:
        raise RuntimeError(f"Requête Flux échouée : {resultat.stderr.strip()}")
    return _parser_csv_annote(resultat.stdout)


def pg_connect():
    """Connexion TimescaleDB — autocommit désactivé, commit explicite par lot."""
    return psycopg2.connect(
        host=TIMESCALE_HOST,
        port=TIMESCALE_PORT,
        dbname=TIMESCALE_DB,
        user=TIMESCALE_USER,
        password=TIMESCALE_PASSWORD,
    )


def _valeurs_distinctes(mesure: str, tag: str) -> list[str]:
    """Liste des valeurs distinctes d'un tag de découpage (schema.tagValues)."""
    flux = f"""
import "influxdata/influxdb/schema"
schema.tagValues(bucket: "{INFLUX_BUCKET}", tag: "{tag}",
  predicate: (r) => r._measurement == "{mesure}")
"""
    return [ligne["_value"] for ligne in flux_query(flux)]


def _premier_dernier_jour(
    mesure: str, tag_decoupage: str | None, valeur: str | None, champ_reference: str
):
    """Bornes temporelles (first/last) — toujours filtré sur un seul tag/canal
    ET un seul champ à la fois pour rester dans le cas "série unique" rapide
    (règle absolue, cf. docstring de tête de fichier)."""
    if tag_decoupage:
        resultats = {}
        for nom_fn, fn in (("premier", "first"), ("dernier", "last")):
            flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {DATE_DEBUT_HISTORIQUE})
  |> filter(fn: (r) => r._measurement == "{mesure}")
  |> filter(fn: (r) => r.{tag_decoupage} == "{valeur}")
  |> filter(fn: (r) => r._field == "{champ_reference}")
  |> keep(columns: ["_time", "_value"])
  |> {fn}()
"""
            lignes = flux_query(flux)
            if lignes:
                resultats[nom_fn] = datetime.fromisoformat(lignes[0]["_time"])
        return resultats.get("premier"), resultats.get("dernier")

    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {DATE_DEBUT_HISTORIQUE})
  |> filter(fn: (r) => r._measurement == "{mesure}")
  |> filter(fn: (r) => r._field == "{champ_reference}")
  |> keep(columns: ["_time"])
"""
    horodatages = [datetime.fromisoformat(ligne["_time"]) for ligne in flux_query(flux)]
    if not horodatages:
        return None, None
    return min(horodatages), max(horodatages)


def _tags_constants(mesure: str, tag_decoupage: str, valeur: str, tags: list[str]) -> dict:
    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {DATE_DEBUT_HISTORIQUE})
  |> filter(fn: (r) => r._measurement == "{mesure}")
  |> filter(fn: (r) => r.{tag_decoupage} == "{valeur}")
  |> last()
"""
    lignes = flux_query(flux)
    if not lignes:
        return {t: "" for t in tags}
    return {t: lignes[0].get(t, "") for t in tags}


def _points_jour(
    mesure: str, tag_decoupage: str | None, valeur: str | None, champs: list[str], jour: date
) -> list[dict]:
    """Un point par _time, champs pivotés en colonnes — borné à UN jour et UNE
    valeur de tag de découpage à la fois (règle absolue, cf. docstring)."""
    debut = datetime.combine(jour, datetime.min.time(), tzinfo=timezone.utc).isoformat()
    fin = (
        datetime.combine(jour, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
    ).isoformat()
    filtre_tag = f'|> filter(fn: (r) => r.{tag_decoupage} == "{valeur}")' if tag_decoupage else ""
    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {debut}, stop: {fin})
  |> filter(fn: (r) => r._measurement == "{mesure}")
  {filtre_tag}
  |> group(columns: ["_field"])
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
"""
    lignes = flux_query(flux, timeout=60)
    for ligne in lignes:
        ligne["_time"] = datetime.fromisoformat(ligne["_time"])
    return lignes


def _deja_verifie(pg_conn, mesure: str, tag_valeur: str, jour: date) -> bool:
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT verifie FROM migration_progress WHERE mesure=%s AND tag_valeur=%s AND jour=%s",
            (mesure, tag_valeur, jour),
        )
        row = cur.fetchone()
        return bool(row and row[0])


def _migrer_lot(pg_conn, mesure: str, config: dict, tag_valeur: str, jour: date) -> None:
    tag_decoupage = config["tag_decoupage"]
    champs = config["champs"]
    lignes = _points_jour(mesure, tag_decoupage, tag_valeur, champs, jour)
    nb_source = len(lignes)

    tags_constants = {}
    if tag_decoupage:
        tags_constants = _tags_constants(
            mesure, tag_decoupage, tag_valeur, config["tags_constants"]
        )
        colonnes_tags = [tag_decoupage] + config["tags_constants"]
    else:
        colonnes_tags = config.get("tags_par_point", [])

    colonnes = ["time"] + colonnes_tags + champs
    debut = datetime.combine(jour, datetime.min.time(), tzinfo=timezone.utc)
    fin = debut + timedelta(days=1)

    with pg_conn.cursor() as cur:
        # Idempotent : le lot est effacé puis réécrit en entier, jamais fusionné
        # avec un reste d'un essai précédent partiel.
        if tag_decoupage:
            cur.execute(
                f"DELETE FROM {mesure} WHERE {tag_decoupage} = %s AND time >= %s AND time < %s",
                (tag_valeur, debut, fin),
            )
        else:
            cur.execute(f"DELETE FROM {mesure} WHERE time >= %s AND time < %s", (debut, fin))

        if lignes:
            buffer = io.StringIO()
            for point in lignes:
                valeurs = [point["_time"].isoformat()]
                if tag_decoupage:
                    valeurs.append(tag_valeur)
                    valeurs += [tags_constants.get(t, "") for t in config["tags_constants"]]
                else:
                    valeurs += [str(point.get(t, "")) for t in colonnes_tags]
                for champ in champs:
                    v = point.get(champ)
                    valeurs.append("" if v is None else str(v))
                ligne_csv = "\t".join(
                    v.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
                    for v in valeurs
                )
                buffer.write(ligne_csv + "\n")
            buffer.seek(0)
            cur.copy_expert(
                f"COPY {mesure} ({', '.join(colonnes)}) FROM STDIN WITH (FORMAT text, NULL '')",
                buffer,
            )
        cur.execute(
            f"SELECT count(*) FROM {mesure} WHERE "
            + (f"{tag_decoupage} = %s AND " if tag_decoupage else "")
            + "time >= %s AND time < %s",
            ((tag_valeur, debut, fin) if tag_decoupage else (debut, fin)),
        )
        nb_migres = cur.fetchone()[0]
        verifie = nb_migres == nb_source
        cur.execute(
            """
            INSERT INTO migration_progress
                (mesure, tag_valeur, jour, nb_points_source, nb_points_migres, verifie, migre_le)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (mesure, tag_valeur, jour) DO UPDATE SET
                nb_points_source = EXCLUDED.nb_points_source,
                nb_points_migres = EXCLUDED.nb_points_migres,
                verifie = EXCLUDED.verifie,
                migre_le = now()
            """,
            (mesure, tag_valeur or "_", jour, nb_source, nb_migres, verifie),
        )
    pg_conn.commit()

    marqueur = "OK" if verifie else "ECART"
    print(
        f"  [{marqueur}] {mesure} / {tag_valeur or '(global)'} / {jour} : "
        f"{nb_source} source -> {nb_migres} migrés",
        flush=True,
    )


def migrer_mesure(
    mesure: str, canal_filtre: str | None = None, jour_max: date | None = None
) -> None:
    """Migre une mesure entière (ou un seul canal/tag si canal_filtre est fourni),
    jour par jour, en reprenant où un lancement précédent s'était arrêté."""
    config = MESURES[mesure]
    pg_conn = pg_connect()

    tag_decoupage = config["tag_decoupage"]
    if tag_decoupage:
        valeurs = _valeurs_distinctes(mesure, tag_decoupage)
        if canal_filtre:
            valeurs = [v for v in valeurs if v == canal_filtre]
        print(f"{mesure} : {len(valeurs)} valeur(s) de {tag_decoupage} à migrer : {valeurs}")
    else:
        valeurs = [None]

    champ_reference = config["champs"][0]
    for valeur in valeurs:
        premier, dernier = _premier_dernier_jour(mesure, tag_decoupage, valeur, champ_reference)
        if premier is None:
            print(f"  {mesure} / {valeur} : aucune donnée, ignoré.")
            continue
        jour = premier.date()
        fin = min(dernier.date(), jour_max) if jour_max else dernier.date()
        print(f"  {mesure} / {valeur or '(global)'} : {jour} -> {fin}")
        while jour <= fin:
            if not _deja_verifie(pg_conn, mesure, valeur or "_", jour):
                _migrer_lot(pg_conn, mesure, config, valeur, jour)
            jour += timedelta(days=1)

    pg_conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mesure", choices=list(MESURES))
    parser.add_argument(
        "--canal", default=None, help="Limiter à une seule valeur de tag de découpage"
    )
    parser.add_argument(
        "--jour-max", default=None, help="AAAA-MM-JJ — arrêter la migration à ce jour inclus"
    )
    parser.add_argument("--toutes", action="store_true")
    args = parser.parse_args()

    jour_max = date.fromisoformat(args.jour_max) if args.jour_max else None

    if args.toutes:
        for m in MESURES:
            migrer_mesure(m, jour_max=jour_max)
    elif args.mesure:
        migrer_mesure(args.mesure, canal_filtre=args.canal, jour_max=jour_max)
    else:
        parser.error("Préciser --mesure <nom> ou --toutes")
