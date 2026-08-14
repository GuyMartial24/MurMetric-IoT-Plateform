"""Backfill HR/T — import direct des exports historiques BlueMaestro
(dossier data_HR_T/T et HR/) dans InfluxDB.

Contourne volontairement MQTT/Kafka (cf. logique_projet.md) : ~45 000 points
attendus, contre 1,5 milliard pour le retrait — la chaîne MQTT→Kafka existe
pour la résilience d'un flux continu distant et l'absorption d'un volume
massif, aucun des deux ne s'applique à un backfill ponctuel depuis des
fichiers déjà sur disque. Écriture directe en InfluxDB, en réutilisant la
même structure de ligne (tags/fields) que construire_point_capteurs() dans
kafka_consumer_influx.py, pour que backfill et flux live produisent des
points strictement compatibles.

Un seul fichier par capteur (le prélèvement le plus récent disponible pour ce
numéro) : les exports BlueMaestro sont cumulatifs, le dernier contient déjà
tout l'historique des précédents (vérifié empiriquement, cf. rapport HR/T).

Identité provisoire : les capteurs HR/T embarqués (dossier T et HR/) n'ont
jamais été vus par un scan BLE réel — seuls les 4 premiers octets de leur MAC
sont connus (export BlueMaestro / étiquette physique). capteurs.json les
enregistre sous une clé provisoire à 8 caractères hex (mac_complete_connue:
false). Chaque point backfillé porte un field mac_complete_connue=false
supplémentaire pour retrouver facilement, une fois le Raspberry Pi déployé,
tout ce qui reste à réconcilier avec la vraie MAC (mêmes 4 premiers octets,
donc pas de devinette à ce moment-là).

Usage :
    python backfill_hr_t.py                  Aperçu (dry-run) — n'écrit rien
    python backfill_hr_t.py --confirmer       Écriture réelle dans InfluxDB

    Variables d'environnement (mêmes défauts que kafka_consumer_influx.py) :
        INFLUX_URL       URL InfluxDB                          (défaut : http://localhost:8086)
        INFLUX_TOKEN     Token API InfluxDB
        INFLUX_ORG       Organisation InfluxDB                 (défaut : FRD_CODEM)
        INFLUX_BUCKET    Bucket de destination                 (défaut : Test_Capteurs)
        HR_T_SOURCE_DIR  Dossier des prélèvements              (défaut : data_HR_T/T
                                                                 et HR à côté de ce script)
        HR_T_DATE_DEBUT  Date ISO (AAAA-MM-JJ) de début retenue (défaut : 2025-12-01)

    IMPORTANT — HR_T_DATE_DEBUT est une fenêtre GLOBALE approximative (début
    de campagne connu, cf. logique_projet.md), pas la date d'installation
    réelle par position — certains capteurs ont un historique interne
    antérieur à leur pose en paroi (jusqu'à mai 2025 constaté). Affiner cette
    date par position avant une écriture réelle si une source plus précise
    est disponible (PowerPoint des maquettes, équipe terrain).
"""

import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Configuration.
# ---------------------------------------------------------------------------
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "MON_TOKEN_API_GENERE_PAR_INFLUXDB")
INFLUX_ORG = os.getenv("INFLUX_ORG", "FRD_CODEM")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Test_Capteurs")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HR_T_SOURCE_DIR = os.getenv("HR_T_SOURCE_DIR", os.path.join(SCRIPT_DIR, "data_HR_T", "T et HR"))
CAPTEURS_FILE = os.path.join(SCRIPT_DIR, "capteurs.json")

DATE_DEBUT = datetime.strptime(os.getenv("HR_T_DATE_DEBUT", "2025-12-01"), "%Y-%m-%d").replace(
    tzinfo=timezone.utc
)

CONFIRMER = "--confirmer" in sys.argv

MESURE_CAPTEURS = "mesures_capteurs"

RE_NOUVEAU = re.compile(r"^(\d+)_([0-9A-Fa-f]+)_log\.csv$")
RE_ANCIEN = re.compile(r"^(\d+)(?:\s*\(\d+\))?\.csv$")
RE_GMT_OFFSET = re.compile(r"GMT([+-])(\d{2})(\d{2})")


def _echap_tag(valeur: str) -> str:
    """Échapper une valeur de tag pour le line protocol — identique à
    kafka_consumer_influx.py, dupliqué ici pour ne pas importer ce module
    (son import déclencherait sa connexion Kafka/InfluxDB au chargement)."""
    return valeur.replace("\\", "\\\\").replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


# ---------------------------------------------------------------------------
# Registre des capteurs HR/T (sous-ensemble de capteurs.json).
# ---------------------------------------------------------------------------


def charger_registre_hr_t() -> dict:
    """Charge capteurs.json, ne garde que les entrées HR/T (marquées par le
    champ numero_capteur_hr_t, absent des autres capteurs BLE du fichier)."""
    with open(CAPTEURS_FILE, "r", encoding="utf-8-sig") as f:
        donnees = json.load(f)
    return {
        cle.upper(): infos
        for cle, infos in donnees.items()
        if not cle.startswith("_") and "numero_capteur_hr_t" in infos
    }


# ---------------------------------------------------------------------------
# Localisation du dernier prélèvement disponible par capteur.
# ---------------------------------------------------------------------------


def _date_dossier(nom: str) -> str:
    """'prélèvement 21-01-2026' -> '2026-01-21' (triable chronologiquement)."""
    m = re.search(r"(\d{2})-(\d{2})-(\d{4})", nom)
    if m:
        j, mo, a = m.groups()
        return f"{a}-{mo}-{j}"
    return nom


def trouver_dernier_fichier_par_slot() -> dict:
    """Parcourt HR_T_SOURCE_DIR, retourne {numero_slot: (chemin, format)}
    pour le prélèvement le plus récent de chaque capteur numéroté. Les
    exports étant cumulatifs (vérifié, cf. rapport), le plus récent contient
    déjà tout l'historique des précédents — inutile de les lire tous."""
    meilleurs = {}
    for dossier, _, fichiers in os.walk(HR_T_SOURCE_DIR):
        nom_dossier = os.path.basename(dossier)
        if not nom_dossier.lower().startswith(("prél", "prélè", "prélé")):
            continue
        for nom_fichier in fichiers:
            if not nom_fichier.lower().endswith(".csv"):
                continue
            m_nouveau = RE_NOUVEAU.match(nom_fichier)
            m_ancien = RE_ANCIEN.match(nom_fichier)
            if m_nouveau:
                slot, fmt = int(m_nouveau.group(1)), "nouveau"
            elif m_ancien:
                slot, fmt = int(m_ancien.group(1)), "ancien"
            else:
                continue  # Nom non standard (3 cas connus, cf. rapport) — ignoré.

            chemin = os.path.join(dossier, nom_fichier)
            candidat_date = _date_dossier(nom_dossier)
            actuel = meilleurs.get(slot)
            if actuel is None or candidat_date > actuel[2]:
                meilleurs[slot] = (chemin, fmt, candidat_date)
    return {slot: (chemin, fmt) for slot, (chemin, fmt, _) in meilleurs.items()}


# ---------------------------------------------------------------------------
# Lecture des mesures (les deux formats de CSV).
# ---------------------------------------------------------------------------


def _parser_date_ancien(chaine: str):
    """Format ancien : date locale + fuseau explicite en texte, ex.
    'Mon Mar 17 2025 17:24:43 GMT+0100 (heure normale d'Europe centrale)'.
    Convertit en UTC réel — un simple retrait du suffixe GMT+HHMM sans
    appliquer le décalage produirait une erreur systématique d'1 à 2h."""
    m = RE_GMT_OFFSET.search(chaine)
    if not m:
        return None
    signe, hh, mm = m.groups()
    decalage_min = int(hh) * 60 + int(mm)
    if signe == "-":
        decalage_min = -decalage_min
    partie_date = chaine.split("GMT")[0].strip()
    try:
        d_locale = datetime.strptime(partie_date, "%a %b %d %Y %H:%M:%S")
    except ValueError:
        return None
    return (d_locale - timedelta(minutes=decalage_min)).replace(tzinfo=timezone.utc)


def _vers_float(valeur: str, decimale_virgule: bool) -> float:
    return float(valeur.replace(",", ".")) if decimale_virgule else float(valeur)


def lire_mesures(chemin: str, fmt: str):
    """Génère (datetime_utc, temperature, humidite, point_de_rosee|None)
    pour chaque ligne valide du fichier.

    Format "nouveau" — deux variantes rencontrées selon les paramètres
    régionaux du poste ayant fait l'export BlueMaestro (43 fichiers du
    08/01/2026, un exportateur différent des autres lots, cf. rapport) :
    virgule + décimale point (standard), ou point-virgule + décimale
    virgule (Excel régional FR). Détection automatique par la ligne d'en-tête
    de données ("index,Date..." vs "index;Date...") plutôt qu'un séparateur
    fixe — un `csv.reader` réglé sur ',' ignore silencieusement tout un
    fichier ';' (une seule "colonne" par ligne, jamais assez de champs).
    """
    with open(chemin, "r", encoding="utf-8-sig", errors="replace") as f:
        lignes = f.read().splitlines()

    if fmt == "nouveau":
        delimiteur = None
        idx_data = None
        for i, ligne in enumerate(lignes):
            if ligne.startswith("index,"):
                delimiteur, idx_data = ",", i
                break
            if ligne.startswith("index;"):
                delimiteur, idx_data = ";", i
                break
        if idx_data is None:
            return
        decimale_virgule = delimiteur == ";"
        for ligne in lignes[idx_data + 1 :]:
            parts = next(csv.reader([ligne], delimiter=delimiteur), None)
            if not parts or len(parts) < 5:
                continue
            try:
                d = datetime.strptime(parts[1][:19], "%Y-%m-%dT%H:%M:%S").replace(
                    tzinfo=timezone.utc
                )
                t = _vers_float(parts[3], decimale_virgule)
                h = _vers_float(parts[4], decimale_virgule)
            except (ValueError, IndexError):
                continue
            dp = None
            try:
                dp = _vers_float(parts[5], decimale_virgule)
            except (ValueError, IndexError):
                pass
            yield d, t, h, dp
    else:
        for parts in csv.reader(lignes):
            if not parts or not parts[0].strip().lstrip("-").isdigit():
                continue
            d = _parser_date_ancien(parts[1]) if len(parts) > 1 else None
            if d is None:
                continue
            try:
                t = float(parts[2])
                h = float(parts[3])
            except (ValueError, IndexError):
                continue
            dp = None
            try:
                dp = float(parts[4])
            except (ValueError, IndexError):
                pass
            yield d, t, h, dp


# ---------------------------------------------------------------------------
# Construction des lignes InfluxDB (line protocol).
# ---------------------------------------------------------------------------


def construire_ligne(infos: dict, d: datetime, t: float, h: float, dp) -> str:
    """Même structure de tags que construire_point_capteurs()
    (kafka_consumer_influx.py) — garantit des points compatibles avec le
    futur flux live. mac_complete_connue=false marque le point comme
    provisoire (cf. docstring du module)."""
    tags = (
        f"adresse_mac={_echap_tag(infos['mac'])},"
        f"emplacement={_echap_tag(infos.get('emplacement') or 'Non défini')},"
        f"nom_capteur={_echap_tag(infos.get('nom') or 'Inconnu')},"
        f"nom_couche={_echap_tag(infos.get('nom_couche') or 'Non défini')},"
        f"nom_mur={_echap_tag(infos.get('nom_mur') or 'Non défini')},"
        f"position={_echap_tag(infos.get('position') or 'Non défini')},"
        f"rd={_echap_tag(infos.get('categorie R&D') or 'Non défini')}"
    )
    fields = f"temperature={t},humidite={h},mac_complete_connue=false"
    if dp is not None:
        fields += f",point_de_rosee={dp}"
    ts_ns = int(d.timestamp() * 1_000_000_000)
    return f"{MESURE_CAPTEURS},{tags} {fields} {ts_ns}"


# ---------------------------------------------------------------------------
# Programme principal.
# ---------------------------------------------------------------------------


def main() -> None:
    """Point d'entrée : lit les CSV HR/T, résume les points trouvés, écrit dans InfluxDB
    seulement si --confirmer est passé (dry-run par défaut)."""
    registre = charger_registre_hr_t()
    print(f"{len(registre)} capteurs HR/T dans capteurs.json.")

    fichiers = trouver_dernier_fichier_par_slot()
    print(f"{len(fichiers)} numéros de capteur avec au moins un fichier CSV trouvé.")
    print(f"Fenêtre retenue : à partir du {DATE_DEBUT.date()} (HR_T_DATE_DEBUT).\n")

    lignes = []
    for hexid_maj, infos in sorted(registre.items(), key=lambda kv: kv[1]["numero_capteur_hr_t"]):
        num = infos["numero_capteur_hr_t"]
        if num not in fichiers:
            print(f"  ⚠️  Capteur {num:3d} ({hexid_maj}) : aucun fichier CSV trouvé, ignoré.")
            continue
        chemin, fmt = fichiers[num]
        n = 0
        for d, t, h, dp in lire_mesures(chemin, fmt):
            if d < DATE_DEBUT:
                continue
            lignes.append(construire_ligne(infos, d, t, h, dp))
            n += 1
        print(
            f"  Capteur {num:3d} ({hexid_maj}) -> "
            f"{infos['nom_mur']} / {infos['position']} / {infos['nom_couche']} : "
            f"{n} points ({os.path.basename(os.path.dirname(chemin))})"
        )

    print(
        f"\nTotal : {len(lignes)} points à écrire dans '{MESURE_CAPTEURS}' "
        f"(bucket {INFLUX_BUCKET})."
    )

    if not CONFIRMER:
        print("\nAperçu uniquement (dry-run) — rien n'a été écrit dans InfluxDB.")
        print("Relancer avec --confirmer pour une écriture réelle.")
        if lignes:
            print("\nExemple de ligne générée :")
            print(" ", lignes[0])
        return

    print("\nÉcriture dans InfluxDB (mode synchrone)...")
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    TAILLE_LOT = 5000
    try:
        for i in range(0, len(lignes), TAILLE_LOT):
            lot = lignes[i : i + TAILLE_LOT]
            write_api.write(
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
                record=lot,
                write_precision=WritePrecision.NS,
            )
            print(f"  {min(i + TAILLE_LOT, len(lignes))}/{len(lignes)} points écrits.")
    finally:
        write_api.close()
        client.close()
    print("Terminé.")


if __name__ == "__main__":
    main()
