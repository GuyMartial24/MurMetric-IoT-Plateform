"""Requêtage InfluxDB générique pour les 3 types de mesures du projet
(HR/T capteurs BLE, retrait DeweSoft, teneur en eau) — alimente à la fois
l'abaque (vue d'ensemble) et l'agrégation utilisée par l'assistant IA."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from .. import config
from ..hampel import appliquer_bornes_physiques, filtrer_hampel
from ..influx import (
    MESURE_CAPTEURS,
    MESURE_DEWESOFT,
    MESURE_TENEUR_EAU,
    flux_escape,
    query_api,
)

router = APIRouter(prefix="/api/mesures", tags=["mesures"])

TypeMesure = Literal["hr_t", "retrait", "teneur_eau"]

_MESURE_PAR_TYPE = {
    "hr_t": MESURE_CAPTEURS,
    "retrait": MESURE_DEWESOFT,
    "teneur_eau": MESURE_TENEUR_EAU,
}
_CHAMPS_PAR_TYPE = {
    "hr_t": ["temperature", "humidite", "point_de_rosee"],
    "retrait": ["valeur", "valeur_filtree"],
    "teneur_eau": ["teneur_eau_pourcent"],
}


# Fenêtre par défaut quand ni `debut` ni `fin` ne sont fournis — 30 jours
# pour "retrait" seulement : mesures_dewesoft est ~1,5 milliard de points
# (échantillonnage 100 Hz, cf. section 32), une agrégation sur 365 jours y
# dépasse régulièrement 30s même avec un seul canal filtré (testé en
# conditions réelles le 12/08/2026, sur ce VPS partagé sans GPU). hr_t/
# teneur_eau restent sur 365 jours : volumes négligeables (dizaines de
# milliers de points), la requête est instantanée quelle que soit la plage.
_FENETRE_DEFAUT_JOURS = {"hr_t": 365, "retrait": 30, "teneur_eau": 365}


def _valider_bornes(
    debut: str | None, fin: str | None, type_mesure: TypeMesure = "hr_t"
) -> tuple[str, str]:
    """Retourne (debut, fin) en RFC3339 complet (avec fuseau) — les champs
    <input type="date"> du frontend (ex. "2026-06-01") produisent un
    datetime NAÏF via fromisoformat() (pas de composante horaire/fuseau) :
    son .isoformat() ne se termine ni par "Z" ni par un offset, ce que le
    parseur Flux ne reconnaît pas comme un littéral temporel valide dans
    range(start: ..., stop: ...) — bug trouvé le 14/08/2026 (erreur de
    compilation Flux en cascade dès qu'une période était choisie via les
    champs Début/Fin). Toute date/heure naïve est donc explicitement
    fixée en UTC avant sérialisation."""
    fin_dt = (
        datetime.fromisoformat(fin.replace("Z", "+00:00"))
        if fin
        else datetime.now(timezone.utc)
    )
    if fin_dt.tzinfo is None:
        fin_dt = fin_dt.replace(tzinfo=timezone.utc)
    jours = _FENETRE_DEFAUT_JOURS[type_mesure]
    debut_dt = (
        datetime.fromisoformat(debut.replace("Z", "+00:00"))
        if debut
        else fin_dt - timedelta(days=jours)
    )
    if debut_dt.tzinfo is None:
        debut_dt = debut_dt.replace(tzinfo=timezone.utc)
    return debut_dt.isoformat(), fin_dt.isoformat()


def construire_requete_flux(
    type_mesure: TypeMesure,
    mur: str | None,
    couche: str | None,
    position: str | None,
    canal_nom: str | None,
    debut: str,
    fin: str,
    fenetre: str | None,
) -> str:
    """Construit la requête Flux de courbe pour une sélection mur/couche/position/canal."""
    mesure = _MESURE_PAR_TYPE[type_mesure]
    champs = _CHAMPS_PAR_TYPE[type_mesure]

    filtres = [f'r._measurement == "{mesure}"']
    filtres.append("(" + " or ".join(f'r._field == "{c}"' for c in champs) + ")")
    if mur:
        filtres.append(
            f'r.nom_mur == "{flux_escape(mur)}"'
            if type_mesure != "teneur_eau"
            else f'r.mur == "{flux_escape(mur)}"'
        )
    if couche:
        filtres.append(
            f'r.nom_couche == "{flux_escape(couche)}"'
            if type_mesure != "teneur_eau"
            else f'r.couche == "{flux_escape(couche)}"'
        )
    if position and type_mesure in ("hr_t", "retrait"):
        filtres.append(f'r.position == "{flux_escape(position)}"')
    if canal_nom and type_mesure == "retrait":
        filtres.append(f'r.canal_nom == "{flux_escape(canal_nom)}"')

    clause_filtre = (
        "\n  |> filter(fn: (r) => " + ")\n  |> filter(fn: (r) => ".join(filtres) + ")"
    )
    agregation = (
        f"\n  |> aggregateWindow(every: {fenetre}, fn: mean, createEmpty: false)"
        if fenetre
        else ""
    )

    # group(columns: ["_field"]) : sans position/canal_nom précisé, une
    # sélection mur+couche peut recouper plusieurs capteurs physiques
    # distincts (ex. "SOCMA 1"+"Milieu carreau" recoupe 2 capteurs, vérifié
    # en direct le 14/08/2026) — chacun forme sa PROPRE table InfluxDB, sort()
    # trie CHRONOLOGIQUEMENT À L'INTÉRIEUR DE CHAQUE TABLE mais ne fusionne
    # jamais les tables entre elles. executer_requete() concatène les tables
    # telles quelles (table 0 en entier, puis table 1 en entier) : la courbe
    # tracée saute d'un coup du dernier point du capteur A au premier point
    # du capteur B, produisant un long trait diagonal qui traverse le
    # graphique — bug trouvé le 14/08/2026 en vérifiant le diagnostic de
    # l'assistant IA sur exactement cet artefact visuel. group(columns:
    # ["_field"]) fusionne les tables par grandeur (température/humidité/...
    # restent séparées, correctement agrégées si aggregateWindow est
    # utilisé) mais mélange les capteurs au sein d'une même grandeur, ce que
    # sort() peut alors trier en une seule fois sur l'ensemble — la même
    # correction que pour les agrégats de l'assistant (section 32, correctif
    # du 13/08/2026), appliquée ici au tracé plutôt qu'aux statistiques.
    return (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: {debut}, stop: {fin})"
        f"{clause_filtre}\n"
        f'  |> group(columns: ["_field"])'
        f"{agregation}\n"
        f'  |> sort(columns: ["_time"])'
    )


def executer_requete(flux: str) -> list[dict]:
    """Exécute une requête Flux et aplatit les résultats en liste de dicts (une erreur
    InfluxDB devient une 502, jamais une 500 brute)."""
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:  # connexion InfluxDB indisponible, requête invalide...
        raise HTTPException(
            status_code=502, detail=f"Requête InfluxDB échouée : {exc}"
        ) from exc

    resultats = []
    for table in tables:
        for record in table.records:
            valeurs = record.values
            point = {
                "time": record.get_time().isoformat(),
                "field": record.get_field(),
                "value": record.get_value(),
            }
            for tag in (
                "nom_mur",
                "mur",
                "nom_couche",
                "couche",
                "position",
                "canal_nom",
                "utilisateur_nom",
                "commentaire",
            ):
                if tag in valeurs:
                    point[tag] = valeurs[tag]
            resultats.append(point)
    return resultats


_AGREGATS_NATIFS = (
    ("minimum", "min"),
    ("maximum", "max"),
    ("moyenne", "mean"),
    ("mediane", "median"),
    ("nombre_points", "count"),
)


def _construire_filtres_communs(
    type_mesure: TypeMesure, mur, couche, position, canal_nom
) -> list[str]:
    filtres = []
    if mur:
        filtres.append(
            f'r.nom_mur == "{flux_escape(mur)}"'
            if type_mesure != "teneur_eau"
            else f'r.mur == "{flux_escape(mur)}"'
        )
    if couche:
        filtres.append(
            f'r.nom_couche == "{flux_escape(couche)}"'
            if type_mesure != "teneur_eau"
            else f'r.couche == "{flux_escape(couche)}"'
        )
    if position and type_mesure in ("hr_t", "retrait"):
        filtres.append(f'r.position == "{flux_escape(position)}"')
    if canal_nom and type_mesure == "retrait":
        filtres.append(f'r.canal_nom == "{flux_escape(canal_nom)}"')
    return filtres


def _valeur_agregat(
    mesure: str,
    champ: str,
    filtres_communs: list[str],
    debut: str,
    fin: str,
    nom_fonction: str,
):
    """Un agrégat natif InfluxDB (min/max/mean/median/count) — nettement
    plus rapide qu'un reduce() générique passé par la VM Flux point par
    point, testé en conditions réelles le 12/08/2026 sur mesures_dewesoft/
    retrait (~1,5 milliard de points) : le reduce() dépassait encore le
    timeout là où ces agrégats natifs passent."""
    filtres = [
        f'r._measurement == "{mesure}"',
        f'r._field == "{champ}"',
        *filtres_communs,
    ]
    clause_filtre = (
        "\n  |> filter(fn: (r) => " + ")\n  |> filter(fn: (r) => ".join(filtres) + ")"
    )
    # group() sans argument : fusionne toutes les tables restantes en une
    # seule avant l'agrégat. Bug trouvé le 13/08/2026 en déboguant
    # amplitude_jour_nuit : sans lui, une sélection mur+couche qui recoupe
    # plusieurs capteurs (differents "position"/adresse_mac non précisés
    # dans la sélection — ex. "SOCMA 1" + "interface carreau et exterieur"
    # recoupe 2 capteurs à des positions différentes) produit une table par
    # capteur ; l'ancien code ne lisait que la PREMIÈRE table rencontrée,
    # donc une "moyenne" (ou min/max/count) silencieusement calculée sur un
    # seul capteur sur N — jamais détecté avant faute de sélection testée
    # avec plusieurs capteurs sur le même mur+couche. executer_requete()
    # (courbe affichée) n'a jamais eu ce problème : il itère déjà toutes
    # les tables.
    flux = (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: {debut}, stop: {fin})"
        f"{clause_filtre}\n"
        f"  |> group()\n"
        f"  |> {nom_fonction}()"
    )
    tables = query_api().query(flux, org=config.INFLUX_ORG)
    for table in tables:
        for record in table.records:
            return record.get_value()
    return None


def _tendance(
    mesure: str, champ: str, filtres_communs: list[str], debut: str, fin: str
) -> dict | None:
    """Moyenne de la première moitié vs deuxième moitié de la période —
    indicateur de tendance simple (pas une vraie régression linéaire,
    volontairement : reste une comparaison de 2 agrégats natifs, pas un
    rapatriement de série temporelle + calcul en Python)."""
    debut_dt = datetime.fromisoformat(debut)
    fin_dt = datetime.fromisoformat(fin)
    milieu = (debut_dt + (fin_dt - debut_dt) / 2).isoformat()
    premiere = _valeur_agregat(mesure, champ, filtres_communs, debut, milieu, "mean")
    seconde = _valeur_agregat(mesure, champ, filtres_communs, milieu, fin, "mean")
    if premiere is None or seconde is None:
        return None
    return {
        "premiere_moitie": premiere,
        "deuxieme_moitie": seconde,
        "delta": seconde - premiere,
    }


def _amplitude_jour_nuit(
    mesure: str, champ: str, filtres_communs: list[str], debut: str, fin: str
) -> dict | None:
    """Moyenne "jour" (8h-19h) vs "nuit" (20h-7h) — approximé en heures
    UTC (pas de conversion de fuseau horaire : décalage de 1-2h selon
    l'heure d'été/hiver par rapport à l'heure locale d'Amiens, acceptable
    pour un indicateur d'amplitude, pas une donnée horaire précise). Les
    deux bornes de hourSelection() sont INCLUSIVES et son "stop" doit
    rester entre 0 et 23 (24 rejeté par InfluxDB) : jour = [8,19] (12h),
    nuit = [20,23] ∪ [0,7] (12h) — bornes choisies pour ne se chevaucher
    nulle part plutôt que de compter une heure sur les deux périodes.
    "Nuit" = union de deux plages : hourSelection() ne boucle pas
    nativement à travers minuit."""
    clause_filtre = (
        "\n  |> filter(fn: (r) => "
        + ")\n  |> filter(fn: (r) => ".join(
            [
                f'r._measurement == "{mesure}"',
                f'r._field == "{champ}"',
                *filtres_communs,
            ]
        )
        + ")"
    )
    # group() avant chaque mean() : même correctif que _valeur_agregat
    # (fusionner toutes les tables — un capteur par "position"/adresse_mac
    # différente — avant l'agrégat, pas seulement lire la première).
    flux = f"""
jour = from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: {debut}, stop: {fin})
  {clause_filtre}
  |> hourSelection(start: 8, stop: 19)
  |> group()
  |> mean()
  |> set(key: "periode", value: "jour")

nuit1 = from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: {debut}, stop: {fin})
  {clause_filtre}
  |> hourSelection(start: 20, stop: 23)

nuit2 = from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: {debut}, stop: {fin})
  {clause_filtre}
  |> hourSelection(start: 0, stop: 7)

nuit = union(tables: [nuit1, nuit2])
  |> group()
  |> mean()
  |> set(key: "periode", value: "nuit")

union(tables: [jour, nuit])
"""
    valeurs: dict = {}
    tables = query_api().query(flux, org=config.INFLUX_ORG)
    for table in tables:
        for record in table.records:
            valeurs[record.values.get("periode")] = record.get_value()
    if "jour" not in valeurs or "nuit" not in valeurs:
        return None
    return {
        "moyenne_jour": valeurs["jour"],
        "moyenne_nuit": valeurs["nuit"],
        "amplitude": abs(valeurs["jour"] - valeurs["nuit"]),
    }


def calculer_statistiques(
    type_mesure: TypeMesure,
    mur: str | None,
    couche: str | None,
    position: str | None,
    canal_nom: str | None,
    debut: str,
    fin: str,
) -> dict:
    """Stats pré-agrégées — jamais de points bruts envoyés à l'assistant
    IA, cf. section 32 (garde-fou coût/fiabilité).

    Un champ par grandeur du type (ex. hr_t → temperature/humidite/
    point_de_rosee), pas seulement la première — bug trouvé le 13/08/2026 :
    l'assistant ne calculait que sur `_CHAMPS_PAR_TYPE[type][0]`
    (toujours "temperature" pour hr_t), donc ne pouvait littéralement pas
    répondre sur l'humidité ou le point de rosée quel que soit le contenu
    de la sélection ou de la question posée.

    Par champ : minimum/maximum/moyenne/mediane/nombre_points (agrégats
    natifs), tendance (1ère vs 2e moitié de la période), et pour hr_t
    uniquement amplitude_jour_nuit — enrichissements du 13/08/2026, tous
    calculés côté InfluxDB (jamais de série temporelle rapatriée en
    Python). Coût négligeable : hr_t/teneur_eau sont des volumes
    instantanés (cf. _FENETRE_DEFAUT_JOURS), retrait n'a que 2 champs —
    toutes les requêtes (par champ × par enrichissement) partent en
    parallèle sur un seul pool de threads.
    """
    champs = _CHAMPS_PAR_TYPE[type_mesure]
    mesure = _MESURE_PAR_TYPE[type_mesure]
    filtres_communs = _construire_filtres_communs(
        type_mesure, mur, couche, position, canal_nom
    )

    stats_par_champ: dict = {champ: {} for champ in champs}
    try:
        with ThreadPoolExecutor(max_workers=8 * len(champs)) as executor:
            futurs_agregats = {
                (champ, nom): executor.submit(
                    _valeur_agregat, mesure, champ, filtres_communs, debut, fin, fn
                )
                for champ in champs
                for nom, fn in _AGREGATS_NATIFS
            }
            futurs_tendance = {
                champ: executor.submit(
                    _tendance, mesure, champ, filtres_communs, debut, fin
                )
                for champ in champs
            }
            futurs_amplitude = (
                {
                    champ: executor.submit(
                        _amplitude_jour_nuit, mesure, champ, filtres_communs, debut, fin
                    )
                    for champ in champs
                }
                if type_mesure == "hr_t"
                else {}
            )

            for (champ, nom), futur in futurs_agregats.items():
                stats_par_champ[champ][nom] = futur.result()
            for champ, futur in futurs_tendance.items():
                stats_par_champ[champ]["tendance"] = futur.result()
            for champ, futur in futurs_amplitude.items():
                stats_par_champ[champ]["amplitude_jour_nuit"] = futur.result()
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Requête statistique échouée : {exc}"
        ) from exc

    return {
        "champs": stats_par_champ,
        "mur": mur,
        "couche": couche,
        "debut": debut,
        "fin": fin,
    }


def comparer_periodes(
    type_mesure: TypeMesure,
    mur: str | None,
    couche: str | None,
    position: str | None,
    canal_nom: str | None,
    debut1: str,
    fin1: str,
    debut2: str,
    fin2: str,
) -> dict:
    """Comparer deux périodes explicites — outil séparé (section 32,
    13/08/2026) appelé par l'assistant IA à la demande, pas calculé
    systématiquement dans calculer_statistiques() (deux fenêtres complètes
    = deux fois plus de requêtes, pas justifié tant que la question posée
    ne porte pas sur une comparaison)."""
    periode1 = calculer_statistiques(
        type_mesure, mur, couche, position, canal_nom, debut1, fin1
    )
    periode2 = calculer_statistiques(
        type_mesure, mur, couche, position, canal_nom, debut2, fin2
    )
    deltas_moyenne = {}
    for champ in _CHAMPS_PAR_TYPE[type_mesure]:
        m1 = periode1["champs"][champ].get("moyenne")
        m2 = periode2["champs"][champ].get("moyenne")
        deltas_moyenne[champ] = (
            (m2 - m1) if (m1 is not None and m2 is not None) else None
        )
    return {
        "periode_1": periode1,
        "periode_2": periode2,
        "delta_moyenne_periode2_moins_periode1": deltas_moyenne,
    }


def ecart_brut_filtre(canal_nom: str | None, debut: str, fin: str) -> dict:
    """Écart moyen absolu entre retrait brut et filtré (mesures_dewesoft
    uniquement) — proxy peu coûteux à ce que donnerait un recalcul Hampel
    complet sur une longue période (hors de portée : l'outil Hampel
    ajustable est plafonné à 2h côté ingestion 100 Hz, cf. section 32,
    "Filtre de Hampel ajustable à la volée" — les fenêtres habituelles de
    l'assistant vont jusqu'à 30 jours pour le retrait). Calcul entièrement
    côté InfluxDB (pivot + map + mean) : aucun point brut ne traverse le
    réseau vers le process Python."""
    if not canal_nom:
        return {
            "erreur": "canal_nom requis (retrait uniquement, un seul canal à la fois)."
        }
    flux = f"""
import "math"

from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: {debut}, stop: {fin})
  |> filter(fn: (r) => r._measurement == "{MESURE_DEWESOFT}")
  |> filter(fn: (r) => r._field == "valeur" or r._field == "valeur_filtree")
  |> filter(fn: (r) => r.canal_nom == "{flux_escape(canal_nom)}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({{_time: r._time, _value: math.abs(x: r.valeur - r.valeur_filtree)}}))
  |> mean()
"""
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Requête écart brut/filtré échouée : {exc}"
        ) from exc
    for table in tables:
        for record in table.records:
            return {
                "canal_nom": canal_nom,
                "ecart_moyen_absolu": record.get_value(),
                "debut": debut,
                "fin": fin,
            }
    return {
        "canal_nom": canal_nom,
        "ecart_moyen_absolu": None,
        "debut": debut,
        "fin": fin,
    }


_TAGS_PAR_TYPE = {
    "hr_t": ["nom_mur", "nom_couche", "position"],
    "retrait": ["nom_mur", "nom_couche", "position", "canal_nom"],
    "teneur_eau": ["mur", "couche"],
}


def _valeurs_tags_retrait() -> list[dict]:
    """Cas particulier : mesures_dewesoft est trop dense (~1,5 milliard de
    points, cf. section 32) pour qu'un group()+count() sur 2 ans reste
    rapide — testé en direct le 12/08/2026, timeout à 30s. capteurs_retrait.json
    est déjà la source de vérité pour le mapping canal→mur/couche/position
    (c'est lui qui pilote le tagging à l'ingestion), donc pas besoin
    d'interroger InfluxDB pour la même information."""
    from .capteurs import (
        _lire_json,
    )  # import différé : évite un cycle au chargement du module

    registre = _lire_json(config.CAPTEURS_RETRAIT_JSON)
    return [
        {
            "nom_mur": infos.get("nom_mur"),
            "nom_couche": infos.get("nom_couche"),
            "position": infos.get("position"),
            "canal_nom": canal,
        }
        for canal, infos in registre.items()
        if canal != "_schema"
    ]


@router.get("/valeurs-tags")
def valeurs_tags(type: TypeMesure = Query(...)) -> dict:
    """Combinaisons mur/couche/position réellement présentes en base — les
    valeurs sont des chaînes libres (ex. "interface carreau et exterieur",
    pas "carreau_ext", cf. logique_projet.md section 32) et pas toujours
    cohérentes en casse ("Milieu carreau" vs "milieu carreau") : plutôt que
    deviner un nom canonique côté frontend, on liste ce qui existe vraiment
    pour peupler des menus déroulants plutôt que des champs texte libres.

    Fenêtre large (2 ans) et pas "30 derniers jours" : les données HR/T et
    teneur en eau actuelles sont un backfill historique (respectivement
    jusqu'à mai et mars 2026, cf. logique_projet.md sections 29/16) — le Pi
    de collecte live n'est pas encore déployé à Amiens — donc une fenêtre
    récente serait vide, pas juste plus rapide."""
    if type == "retrait":
        return {"type": type, "combinaisons": _valeurs_tags_retrait()}

    mesure = _MESURE_PAR_TYPE[type]
    champ_principal = _CHAMPS_PAR_TYPE[type][0]
    tags = _TAGS_PAR_TYPE[type]
    colonnes_flux = (
        "[" + ", ".join(f'"{t}"' for t in tags) + "]"
    )  # Flux veut des guillemets doubles, pas le repr() Python

    flux = (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: -2y)\n"
        f'  |> filter(fn: (r) => r._measurement == "{mesure}")\n'
        f'  |> filter(fn: (r) => r._field == "{champ_principal}")\n'
        f"  |> group(columns: {colonnes_flux})\n"
        f"  |> count()\n"
        f"  |> group()"
    )
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Requête InfluxDB échouée : {exc}"
        ) from exc

    combinaisons = []
    for table in tables:
        for record in table.records:
            entree = {tag: record.values.get(tag) for tag in tags}
            entree["nombre_points"] = record.get_value()
            combinaisons.append(entree)
    return {"type": type, "combinaisons": combinaisons}


@router.get("/statistiques")
def statistiques(
    type: TypeMesure = Query(...),
    mur: str | None = None,
    couche: str | None = None,
    position: str | None = None,
    canal_nom: str | None = None,
    debut: str | None = None,
    fin: str | None = None,
):
    """Statistiques agrégées (min/max/moyenne/médiane/tendance...) pour une sélection."""
    debut_iso, fin_iso = _valider_bornes(debut, fin, type)
    return calculer_statistiques(
        type, mur, couche, position, canal_nom, debut_iso, fin_iso
    )


@router.get("")
def lister_mesures(
    type: TypeMesure = Query(..., description="hr_t | retrait | teneur_eau"),
    mur: str | None = None,
    couche: str | None = None,
    position: str | None = None,
    canal_nom: str | None = None,
    debut: str | None = Query(
        None, description="ISO 8601, défaut : 1 an avant fin (30 jours si type=retrait)"
    ),
    fin: str | None = Query(None, description="ISO 8601, défaut : maintenant"),
    fenetre: str | None = Query(
        None,
        description="Fenêtre d'agrégation Flux, ex. '1h', '1d' — absent = points bruts",
    ),
):
    """Points de courbe (bruts ou agrégés) pour une sélection mur/couche/position/canal."""
    debut_iso, fin_iso = _valider_bornes(debut, fin, type)
    flux = construire_requete_flux(
        type, mur, couche, position, canal_nom, debut_iso, fin_iso, fenetre
    )
    return {"requete_flux": flux, "points": executer_requete(flux)}


@router.get("/croisement")
def croisement(
    type: TypeMesure = Query(
        ...,
        description=(
            "hr_t | retrait — champs de la même mesure uniquement "
            "(ex. temperature/humidite/point_de_rosee)"
        ),
    ),
    mur: str | None = None,
    couche: str | None = None,
    position: str | None = None,
    champ_x: str = Query(...),
    champ_y: str = Query(...),
    champ_z: str | None = Query(
        None,
        description=(
            "Optionnel — 3e grandeur pour un nomogramme 3D "
            "(ex. hr_t : temperature/humidite/point_de_rosee)"
        ),
    ),
    debut: str | None = None,
    fin: str | None = None,
    fenetre: str | None = Query(
        "10m",
        description=(
            "Fenêtre d'agrégation avant croisement — évite des milliers de points "
            "bruts non alignés dans le temps"
        ),
    ),
) -> dict:
    """Points appariés (x, y[, z]) au même horodatage — nomogramme
    (section 32 : portage scopé au croisement de champs d'une même mesure,
    ex. température/humidité/point de rosée ; le croisement avec la
    teneur en eau, mesure distincte et éparse nécessitant une jointure "au
    plus proche dans le temps" cf. section 16, n'est pas dans ce périmètre)."""
    champs_demandes = [champ_x, champ_y] + ([champ_z] if champ_z else [])
    if type == "teneur_eau" or any(
        c not in _CHAMPS_PAR_TYPE[type] for c in champs_demandes
    ):
        raise HTTPException(
            status_code=400,
            detail="champ_x/champ_y/champ_z doivent appartenir à la même mesure (hr_t ou retrait).",
        )

    debut_iso, fin_iso = _valider_bornes(debut, fin, type)
    mesure = _MESURE_PAR_TYPE[type]
    clause_champs = " or ".join(f'r._field == "{c}"' for c in champs_demandes)
    filtres = [f'r._measurement == "{mesure}"', f"({clause_champs})"]
    if mur:
        filtres.append(f'r.nom_mur == "{flux_escape(mur)}"')
    if couche:
        filtres.append(f'r.nom_couche == "{flux_escape(couche)}"')
    if position:
        filtres.append(f'r.position == "{flux_escape(position)}"')
    clause_filtre = (
        "\n  |> filter(fn: (r) => " + ")\n  |> filter(fn: (r) => ".join(filtres) + ")"
    )
    clause_exists = " and ".join(f"exists r.{c}" for c in champs_demandes)

    flux = (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: {debut_iso}, stop: {fin_iso})"
        f"{clause_filtre}\n"
        # group(columns: ["_field"]) : sans position précisée, plusieurs
        # capteurs physiques peuvent partager mur+couche (vérifié en direct
        # le 14/08/2026 — même bug que construire_requete_flux, trouvé en
        # vérifiant le diagnostic de l'assistant IA sur une courbe qui
        # "sautait" entre deux capteurs). Sans ce group(), pivot() opère
        # table par table (une par capteur) et produit des lignes dupliquées
        # par horodatage plutôt qu'une trajectoire unique et cohérente.
        f'  |> group(columns: ["_field"])\n'
        f"  |> aggregateWindow(every: {fenetre}, fn: mean, createEmpty: false)\n"
        f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
        f"  |> filter(fn: (r) => {clause_exists})\n"
        f'  |> sort(columns: ["_time"])'
    )
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Requête InfluxDB échouée : {exc}"
        ) from exc

    points = []
    for table in tables:
        for record in table.records:
            valeurs = record.values
            point = {
                "time": record.get_time().isoformat(),
                "x": valeurs.get(champ_x),
                "y": valeurs.get(champ_y),
            }
            if champ_z:
                point["z"] = valeurs.get(champ_z)
            points.append(point)
    return {
        "champ_x": champ_x,
        "champ_y": champ_y,
        "champ_z": champ_z,
        "points": points,
    }


_MESURES_LIBRES = {
    "hr_t": MESURE_CAPTEURS,
    "retrait": MESURE_DEWESOFT,
    "teneur_eau": MESURE_TENEUR_EAU,
}


def _parser_axe(spec: str) -> tuple[str, str, str | None]:
    """ "hr_t:temperature" ou "retrait:valeur_filtree:HA1" -> (mesure, champ, canal).

    Canal retrait vide ou absent = "tous les canaux" (28/08/2026) — la
    restriction précédente ("Axe retrait sans canal" toujours refusé)
    n'existait que dans CET endpoint : _construire_filtres_communs/
    construire_requete_flux (statistiques, Vue d'ensemble) autorisent déjà
    un canal_nom vide depuis le début, avec le même group()+aggregateWindow
    (mean) que _requeter_axe ci-dessous — donc la même moyenne inter-canaux,
    déjà jugée acceptable ailleurs dans l'appli. Incohérence corrigée plutôt
    que reproduite."""
    morceaux = spec.split(":")
    if len(morceaux) == 2:
        mesure, champ, canal = morceaux[0], morceaux[1], None
    elif len(morceaux) == 3:
        mesure, champ, canal = morceaux[0], morceaux[1], morceaux[2] or None
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Axe invalide : {spec!r} (attendu 'mesure:champ' ou 'mesure:champ:canal')",
        )
    if mesure not in _MESURES_LIBRES or champ not in _CHAMPS_PAR_TYPE[mesure]:
        raise HTTPException(status_code=400, detail=f"Axe invalide : {spec!r}")
    return mesure, champ, canal


def _requeter_axe(
    mesure: str,
    champ: str,
    canal: str | None,
    mur: str,
    couche: str | None,
    debut: str,
    fin: str,
    fenetre: str,
) -> dict[str, float]:
    filtres = [
        f'r._measurement == "{_MESURES_LIBRES[mesure]}"',
        f'r._field == "{champ}"',
    ]
    # teneur_eau porte ses tags mur/couche SANS le préfixe nom_ (cf.
    # teneur_eau.py, _construire_ligne) — distinct de hr_t/retrait
    # (nom_mur/nom_couche), à mapper explicitement plutôt que de supposer un
    # nom de tag commun (27/08/2026, ajout teneur_eau à /croisement-libre).
    tag_mur = "mur" if mesure == "teneur_eau" else "nom_mur"
    tag_couche = "couche" if mesure == "teneur_eau" else "nom_couche"
    if mur:
        filtres.append(f'r.{tag_mur} == "{flux_escape(mur)}"')
    if mesure in ("hr_t", "teneur_eau") and couche:
        filtres.append(f'r.{tag_couche} == "{flux_escape(couche)}"')
    if mesure == "retrait" and canal:
        # "HA1+HA2" (28/08/2026, demande explicite) : moyenne de plusieurs
        # canaux plutôt qu'un seul — même mécanisme que "Tous" (aucun canal,
        # tous mélangés), juste restreint à la liste jointe par "+". Le
        # frontend ne propose que des combinaisons même mur/même orientation
        # (cf. canauxRetrait.js), mais rien ici ne l'impose côté serveur :
        # cette fonction reste générique, agnostique du sens physique.
        canaux = canal.split("+")
        if len(canaux) == 1:
            filtres.append(f'r.canal_nom == "{flux_escape(canaux[0])}"')
        else:
            clause = " or ".join(f'r.canal_nom == "{flux_escape(c)}"' for c in canaux)
            filtres.append(f"({clause})")
    clause_filtre = (
        "\n  |> filter(fn: (r) => " + ")\n  |> filter(fn: (r) => ".join(filtres) + ")"
    )
    # group() : un seul champ déjà filtré ici (contrairement à croisement()
    # qui en pivote plusieurs), donc pas besoin de préciser columns=["_field"]
    # — fusionne simplement les tables de plusieurs capteurs éventuels
    # (mur+couche sans position) avant l'agrégation, même correctif que
    # construire_requete_flux()/croisement() (14/08/2026). Sans lui, le dict
    # ci-dessous écraserait silencieusement la valeur d'un capteur par celle
    # de l'autre à chaque horodatage partagé entre les deux tables, au lieu
    # d'une vraie moyenne combinée.
    flux = (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: {debut}, stop: {fin})"
        f"{clause_filtre}\n"
        f"  |> group()\n"
        f"  |> aggregateWindow(every: {fenetre}, fn: mean, createEmpty: false)"
    )
    tables = query_api().query(flux, org=config.INFLUX_ORG)
    resultat: dict[str, float] = {}
    for table in tables:
        for record in table.records:
            resultat[record.get_time().isoformat()] = record.get_value()
    return resultat


def _fenetre_auto(debut_iso: str, fin_iso: str) -> str:
    """Fenêtre d'agrégation adaptée à l'étendue [debut_iso, fin_iso] — utilisée
    par croisement_libre quand l'appelant n'impose pas de fenêtre explicite.

    Sans ça, une plage large filtrée sur un axe retrait (mesures_dewesoft,
    échantillonnage 100 Hz) restait figée sur la fenêtre par défaut "1h" quelle
    que soit l'étendue demandée — inoffensif tant que le plafond implicite de
    30 jours de _valider_bornes limitait la casse, mais l'ajout de Début/Fin
    au nomogramme (28/08/2026) permet de le contourner explicitement : un
    croisement teneur_eau x retrait sur ~80 jours a mis 7 à 11s à répondre
    (charge serveur variable), déjà proche de l'ancien timeout client de 30s
    (relevé le 28/08/2026, cf. get_client() dans influx.py). Grossir la
    fenêtre pour les plages larges réduit le nombre de points agrégés
    renvoyés (donc le rendu du nomogramme reste lisible) et allège un peu la
    charge de calcul, sans prétendre éliminer le coût de lecture des points
    bruts sous-jacent — d'où aussi le timeout client relevé à 60s en
    complément, pas un correctif de vitesse à lui seul."""
    jours = (datetime.fromisoformat(fin_iso) - datetime.fromisoformat(debut_iso)).days
    if jours <= 2:
        return "15m"
    if jours <= 10:
        return "1h"
    if jours <= 60:
        return "6h"
    if jours <= 200:
        return "1d"
    return "3d"


@router.get("/croisement-libre")
def croisement_libre(
    mur: str = Query(...),
    couche: str | None = None,
    axe_x: str = Query(..., description="'hr_t:champ' ou 'retrait:champ:canal'"),
    axe_y: str | None = Query(
        None,
        description="Optionnel — un axe peut être 'temps', calculé côté frontend, pas ici",
    ),
    axe_z: str | None = Query(None),
    debut: str | None = None,
    fin: str | None = None,
    fenetre: str | None = Query(
        None,
        description=(
            "Fenêtre d'agrégation commune — aligne les points des deux mesures "
            "sur la même grille temporelle. Non fournie : déduite automatiquement "
            "de l'étendue debut/fin (cf. _fenetre_auto)."
        ),
    ),
) -> dict:
    """Croisement libre entre grandeurs de mesures DIFFÉRENTES (HR/T, retrait,
    teneur_eau), demandé explicitement le 13/08/2026 — contrairement à
    /croisement (une seule mesure, pivot direct). teneur_eau ajoutée le
    27/08/2026 malgré des données éparses/manuelles (demande explicite
    utilisateur) : `aggregateWindow(createEmpty: false)` ne produit un point
    QUE pour les fenêtres qui contiennent une vraie mesure, donc son
    éparsité ne casse rien mécaniquement — elle limite juste le nombre de
    points communs au nombre de relevés terrain, ce qui est attendu (à
    utiliser en "Nuage de points" plutôt qu'un tracé interpolé). Les
    mesures ont des fréquences très différentes (HR/T ~toutes les quelques
    heures, retrait 100 Hz, teneur_eau ponctuelle) : chaque axe est
    interrogé séparément, agrégé sur LA MÊME fenêtre, puis aligné en Python
    sur les horodatages communs — un join
    Flux ferait la même chose côté serveur, mais des requêtes séparées
    restent plus sûres (cf. section 32 : combiner plusieurs canaux/mesures
    dans un seul filtre Flux coûte disproportionnellement plus cher que les
    séparer)."""
    axes = (
        [("x", axe_x)]
        + ([("y", axe_y)] if axe_y else [])
        + ([("z", axe_z)] if axe_z else [])
    )
    parses = {nom: _parser_axe(spec) for nom, spec in axes}

    # Sécurité mémoire : dès qu'un axe retrait est impliqué, la fenêtre est
    # plafonnée à 30 jours (cf. _FENETRE_DEFAUT_JOURS) — mais UNIQUEMENT
    # quand ni debut ni fin ne sont fournis (cf. _valider_bornes). Depuis
    # l'ajout de Début/Fin au nomogramme (28/08/2026), ce plafond implicite
    # est facilement contourné par une plage explicite plus large — d'où
    # _fenetre_auto ci-dessous, qui protège la requête elle-même plutôt que
    # de compter sur une borne de durée.
    type_borne: TypeMesure = (
        "retrait" if any(p[0] == "retrait" for p in parses.values()) else "hr_t"
    )
    debut_iso, fin_iso = _valider_bornes(debut, fin, type_borne)
    fenetre_effective = fenetre or _fenetre_auto(debut_iso, fin_iso)

    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futurs = {
                nom: executor.submit(
                    _requeter_axe,
                    mesure,
                    champ,
                    canal,
                    mur,
                    couche,
                    debut_iso,
                    fin_iso,
                    fenetre_effective,
                )
                for nom, (mesure, champ, canal) in parses.items()
            }
            series = {nom: futur.result() for nom, futur in futurs.items()}
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Requête InfluxDB échouée : {exc}"
        ) from exc

    cles = list(series.keys())
    horodatages_communs = set(series[cles[0]])
    for nom in cles[1:]:
        horodatages_communs &= set(series[nom])

    points = []
    for t in sorted(horodatages_communs):
        point = {"time": t, "x": series["x"][t]}
        if "y" in series:
            point["y"] = series["y"][t]
        if "z" in series:
            point["z"] = series["z"][t]
        points.append(point)

    return {"axe_x": axe_x, "axe_y": axe_y, "axe_z": axe_z, "points": points}


_HAMPEL_DUREE_MAX = timedelta(hours=2)


@router.get("/hampel")
def hampel(
    mur: str = Query(...),
    canal_nom: str = Query(...),
    debut: str = Query(...),
    fin: str = Query(...),
    fenetre: int = Query(
        10,
        ge=1,
        le=200,
        description="Demi-largeur de la fenêtre glissante, en échantillons",
    ),
    seuil_k: float = Query(
        8.0,
        gt=0,
        description="Multiplicateur du MAD au-delà duquel un point est aberrant",
    ),
    borne_min: float | None = Query(
        None,
        description=(
            "Borne physique basse optionnelle — rattrape les rafales trop "
            "longues pour le Hampel seul"
        ),
    ),
    borne_max: float | None = Query(
        None, description="Borne physique haute optionnelle"
    ),
) -> dict:
    """Filtre de Hampel recalculé à la volée sur les valeurs BRUTES (jamais
    celles déjà stockées dans `valeur_filtree`, fixées à l'ingestion et pas
    ajustables — demande explicite du 13/08/2026). Volontairement limité à
    une fenêtre courte (2h max) : mesures_dewesoft est à 100 Hz, un
    recalcul point par point sur une période longue reviendrait au même
    risque mémoire déjà rencontré et corrigé pour les requêtes agrégées
    (cf. section 32) — ici on ne peut PAS agréger avant de filtrer, la
    résolution native est nécessaire au calcul lui-même.

    borne_min/borne_max : deuxième couche optionnelle, ajoutée après avoir
    constaté qu'un pic positif extrême échappait au Hampel seul (rafale
    d'échantillons aberrants plus longue que la fenêtre glissante, cf.
    logique_projet.md section 32) — indépendante du contexte statistique
    local, contrairement au Hampel."""
    debut_dt = datetime.fromisoformat(debut.replace("Z", "+00:00"))
    fin_dt = datetime.fromisoformat(fin.replace("Z", "+00:00"))
    if fin_dt - debut_dt > _HAMPEL_DUREE_MAX:
        raise HTTPException(
            status_code=400,
            detail=(
                "Période trop longue pour un recalcul point par point "
                f"(max {_HAMPEL_DUREE_MAX})."
            ),
        )

    flux = (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n'
        f"  |> range(start: {debut}, stop: {fin})\n"
        f'  |> filter(fn: (r) => r._measurement == "{MESURE_DEWESOFT}")\n'
        f'  |> filter(fn: (r) => r._field == "valeur")\n'
        f'  |> filter(fn: (r) => r.nom_mur == "{flux_escape(mur)}")\n'
        f'  |> filter(fn: (r) => r.canal_nom == "{flux_escape(canal_nom)}")\n'
        f'  |> sort(columns: ["_time"])'
    )
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Requête InfluxDB échouée : {exc}"
        ) from exc

    temps, valeurs = [], []
    for table in tables:
        for record in table.records:
            temps.append(record.get_time().isoformat())
            valeurs.append(record.get_value())

    if not valeurs:
        return {
            "points": [],
            "nb_points": 0,
            "nb_aberrants": 0,
            "fenetre": fenetre,
            "seuil_k": seuil_k,
        }

    filtrees, aberrants = filtrer_hampel(valeurs, fenetre, seuil_k)
    if borne_min is not None and borne_max is not None:
        filtrees, aberrants = appliquer_bornes_physiques(
            filtrees, aberrants, borne_min, borne_max
        )
    points = [
        {"time": t, "brut": b, "filtre_ajuste": f, "aberrant": a}
        for t, b, f, a in zip(temps, valeurs, filtrees, aberrants)
    ]
    return {
        "points": points,
        "nb_points": len(points),
        "nb_aberrants": sum(aberrants),
        "fenetre": fenetre,
        "seuil_k": seuil_k,
    }
