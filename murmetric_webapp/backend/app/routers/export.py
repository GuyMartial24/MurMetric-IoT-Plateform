"""Export en masse des données de mesure (retrait/HR-T/teneur en eau) en
CSV ou Parquet — distinct du mécanisme d'export de courbe existant
(BoutonsExportDonnees, qui exporte les points déjà chargés dans le
navigateur, adapté à un affichage, pas à un vrai export en masse).

Deux incidents de production le 18/08/2026 (cf. logique_projet.md section
34) en testant une première version qui chargeait tout le résultat en
mémoire via `query_api().query()` (fait planter le pod webapp deux fois,
même après avoir remonté sa limite mémoire 256Mi -> 1Gi) ont mené à cette
refonte, avec deux principes stricts :

1. Lecture en flux (`query_api().query_stream()`, pas `.query()`) — jamais
   plus d'un enregistrement InfluxDB matérialisé à la fois, quelle que soit
   la taille du résultat.
2. Requêtes bornées JOUR PAR JOUR (même règle que le reste du projet pour
   mesures_dewesoft) — jamais une seule requête Flux couvrant toute la
   période demandée, et jamais un filtre combinant plusieurs canaux
   (r.canal_nom == "A" or == "B") : chaque canal a son propre générateur,
   fusionnés ensuite par horodatage en mémoire (empreinte O(nombre de
   canaux), pas O(nombre de points)).

Avec ces deux principes, la mémoire utilisée reste à peu près constante
quelle que soit la période demandée — le garde-fou "canal-jours" de la
première version (déjà insuffisant en pratique) devient inutile et a été
retiré. Deux modes de livraison : téléchargement direct (réponse HTTP en
flux, adapté à une période raisonnable) ou tâche de fond avec suivi
d'avancement (fichier écrit progressivement sur le volume persistant,
adapté à une période longue ou une génération répétée sans surveiller)."""

import csv
import threading
import uuid
from collections.abc import Generator, Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from starlette.background import BackgroundTask

from .. import config
from ..auth import utilisateur_courant
from ..influx import (
    MESURE_CAPTEURS,
    MESURE_DEWESOFT,
    MESURE_TENEUR_EAU,
    flux_escape,
    query_api,
    to_rfc3339,
)
from .mesures import _valider_bornes

router = APIRouter(prefix="/api/export", tags=["export"])

_CHAMPS_RETRAIT = {"valeur", "valeur_filtree"}
_FORMATS = {"csv", "parquet"}
# Date de départ documentée pour mesures_dewesoft (logique_projet.md,
# analyse des fichiers .dxd sources) — jamais découverte par une requête
# first() : celle-ci a fait OOM-killer InfluxDB le 17/08/2026 (section 33).
DATE_DEBUT_RETRAIT = date(2025, 11, 1)


# ===========================================================================
# Lecture en flux, jour par jour — cœur de la refonte du 18/08/2026.
# ===========================================================================


def _plage_jours(debut_dt: datetime, fin_dt: datetime) -> Iterator[tuple[datetime, datetime]]:
    """Découpe [debut_dt, fin_dt) en bornes journalières successives — jamais
    une requête non bornée sur mesures_dewesoft, même règle que partout
    ailleurs dans ce projet."""
    courant = debut_dt
    while courant < fin_dt:
        lendemain = datetime.combine(
            courant.date() + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
        )
        borne_fin = min(lendemain, fin_dt)
        yield courant, borne_fin
        courant = borne_fin


def _iterer_canal_retrait(
    canal: str, champ: str, debut_dt: datetime, fin_dt: datetime, resolution: str
) -> Iterator[tuple[datetime, float]]:
    """Générateur (temps, valeur) pour UN canal — requêtes bornées jour par
    jour, jamais combinée avec un autre canal. `query_stream()` (pas
    `.query()`) : ne matérialise jamais tout le résultat d'un coup."""
    agregation = ""
    if resolution in ("heure", "jour"):
        pas = "1h" if resolution == "heure" else "1d"
        agregation = f"\n  |> aggregateWindow(every: {pas}, fn: mean, createEmpty: false)"
    for debut_jour, fin_jour in _plage_jours(debut_dt, fin_dt):
        flux = f"""
from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: {to_rfc3339(debut_jour)}, stop: {to_rfc3339(fin_jour)})
  |> filter(fn: (r) => r._measurement == "{MESURE_DEWESOFT}")
  |> filter(fn: (r) => r._field == "{champ}")
  |> filter(fn: (r) => r.canal_nom == "{flux_escape(canal)}"){agregation}
"""
        for record in query_api().query_stream(flux, org=config.INFLUX_ORG):
            yield record.get_time(), record.get_value()


def _fusionner_par_temps(
    generateurs: list[Iterator[tuple[datetime, float]]],
) -> Iterator[list]:
    """Fusionne N générateurs (temps, valeur) triés par temps en lignes
    [temps, v0, v1, ..., vN-1] — empreinte mémoire O(N), jamais O(nombre de
    points), quelle que soit la période. Tolère un horodatage absent sur un
    canal (cellule vide) sans désynchroniser les autres : les canaux
    retrait partagent la même grille en pratique (DeweSoft les enregistre
    simultanément), mais rien ne l'impose structurellement — mieux vaut un
    trou visible qu'un décalage silencieux."""
    n = len(generateurs)
    courants = [next(g, None) for g in generateurs]
    while any(c is not None for c in courants):
        temps_min = min(c[0] for c in courants if c is not None)
        ligne: list = [temps_min]
        for i in range(n):
            if courants[i] is not None and courants[i][0] == temps_min:
                ligne.append(courants[i][1])
                courants[i] = next(generateurs[i], None)
            else:
                ligne.append(None)
        yield ligne


def _lignes_retrait(
    canaux: list[str], champ: str, debut_dt: datetime, fin_dt: datetime, resolution: str
) -> Iterator[list]:
    generateurs = [
        _iterer_canal_retrait(canal, champ, debut_dt, fin_dt, resolution) for canal in canaux
    ]
    return _fusionner_par_temps(generateurs)


# ===========================================================================
# Sérialisation — CSV en flux direct, Parquet via fichier temporaire (son
# format binaire, avec un pied de fichier écrit à la fin, ne se prête pas à
# un envoi HTTP progressif comme le CSV texte).
# ===========================================================================


class _TamponEcho:
    """Objet "fichier" minimal pour csv.writer : renvoie ce qu'il "écrit" au
    lieu de le stocker — laisse csv.writer gérer l'échappement correctement
    tout en restant un générateur, sans jamais accumuler le fichier entier
    en mémoire."""

    def write(self, valeur: str) -> str:
        return valeur


def _valeur_csv(v):
    if v is None:
        return ""
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def _generer_csv(entetes: list[str], lignes: Iterator[list]) -> Generator[str, None, None]:
    tampon = _TamponEcho()
    ecrivain = csv.writer(tampon)
    # BOM — même raison que exportDonnees.js (Excel Windows).
    yield "﻿"
    yield ecrivain.writerow(entetes)
    for ligne in lignes:
        yield ecrivain.writerow([_valeur_csv(v) for v in ligne])


def _ecrire_parquet(entetes: list[str], lignes: Iterator[list], chemin: Path) -> None:
    """Écrit par lots de 50 000 lignes — mémoire bornée à un lot à la fois,
    pas au fichier entier, comme la lecture InfluxDB en amont."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    taille_lot = 50_000
    ecrivain: pq.ParquetWriter | None = None
    lot: list[list] = []
    try:
        for ligne in lignes:
            lot.append(ligne)
            if len(lot) >= taille_lot:
                table = pa.table({e: c for e, c in zip(entetes, zip(*lot), strict=True)})
                if ecrivain is None:
                    ecrivain = pq.ParquetWriter(chemin, table.schema)
                ecrivain.write_table(table)
                lot = []
        if lot:
            table = pa.table({e: c for e, c in zip(entetes, zip(*lot), strict=True)})
            if ecrivain is None:
                ecrivain = pq.ParquetWriter(chemin, table.schema)
            ecrivain.write_table(table)
        elif ecrivain is None:
            # Aucune ligne du tout — écrit un fichier Parquet valide mais vide
            # (schéma seul), plutôt que rien ou une erreur.
            ecrivain = pq.ParquetWriter(chemin, pa.schema([(e, pa.string()) for e in entetes]))
    finally:
        if ecrivain is not None:
            ecrivain.close()


def _reponse_fichier(chemin: Path, nom_telecharge: str) -> FileResponse:
    """FileResponse envoie le fichier en flux puis exécute la tâche de fond
    (suppression du temporaire) — la suppression après coup ne coupe pas
    l'envoi déjà en cours."""
    return FileResponse(
        chemin,
        media_type="application/octet-stream",
        filename=nom_telecharge,
        background=BackgroundTask(chemin.unlink, missing_ok=True),
    )


# ===========================================================================
# Export retrait — téléchargement direct (réponse HTTP en flux).
# ===========================================================================


@router.get("/retrait")
def exporter_retrait(
    canaux: str = Query(..., description="Séparés par des virgules, ex. 'HA1,HA2,VA1'"),
    champ: str = Query("valeur_filtree", description="valeur | valeur_filtree"),
    debut: str = Query(...),
    fin: str = Query(...),
    resolution: str = Query("heure", description="brut | heure | jour"),
    format: str = Query("csv", description="csv | parquet"),
    _utilisateur: dict = Depends(utilisateur_courant),
):
    """Export "large" : une colonne par canal, une ligne par horodatage —
    les canaux retrait partagent la même grille temporelle (DeweSoft les
    enregistre tous simultanément), la fusion en colonnes est donc directe.
    Réponse en flux : adapté à une période raisonnable — pour une période
    longue, préférer /retrait/tache (génération en arrière-plan, suivi
    d'avancement, téléchargement une fois prête)."""
    liste_canaux = [c.strip() for c in canaux.split(",") if c.strip()]
    if not liste_canaux:
        raise HTTPException(status_code=400, detail="Au moins un canal requis.")
    if champ not in _CHAMPS_RETRAIT:
        raise HTTPException(status_code=400, detail=f"Champ invalide : {champ!r}")
    if resolution not in ("brut", "heure", "jour"):
        raise HTTPException(status_code=400, detail=f"Résolution invalide : {resolution!r}")
    if format not in _FORMATS:
        raise HTTPException(status_code=400, detail=f"Format invalide : {format!r}")

    debut_iso, fin_iso = _valider_bornes(debut, fin, "retrait")
    debut_dt = datetime.fromisoformat(debut_iso.replace("Z", "+00:00"))
    fin_dt = datetime.fromisoformat(fin_iso.replace("Z", "+00:00"))
    entetes = ["temps"] + liste_canaux
    lignes = _lignes_retrait(liste_canaux, champ, debut_dt, fin_dt, resolution)
    nom = f"retrait_{champ}_{resolution}"

    if format == "csv":
        return StreamingResponse(
            _generer_csv(entetes, lignes),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{nom}.csv"'},
        )

    chemin = config.EXPORTS_DIR / f"_tmp_{uuid.uuid4().hex}.parquet"
    config.EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    _ecrire_parquet(entetes, lignes, chemin)
    return _reponse_fichier(chemin, f"{nom}.parquet")


# ===========================================================================
# Export retrait en tâche de fond — même lecture, mais écrite
# progressivement sur le volume persistant plutôt qu'envoyée en flux HTTP,
# avec suivi d'avancement consultable pendant que ça tourne. Utile pour une
# période longue (jusqu'à tout l'historique) ou simplement pour lancer un
# export sans devoir garder l'onglet ouvert jusqu'à la fin.
#
# Suivi en mémoire (dict protégé par verrou), pas en base — acceptable vu le
# déploiement mono-réplique (k8s/webapp/deployment.yaml, replicas: 1) : une
# tâche en cours est perdue si le pod redémarre, à relancer dans ce cas.
# ===========================================================================

_verrou_taches = threading.Lock()
_taches: dict[str, dict] = {}


def _executer_tache_retrait(tache_id: str, canaux, champ, debut_dt, fin_dt, resolution, format):
    jours_total = max(1, (fin_dt.date() - debut_dt.date()).days + 1)
    chemin = config.EXPORTS_DIR / f"{tache_id}.{format}"
    entetes = ["temps"] + canaux

    def lignes_avec_suivi():
        for debut_jour, fin_jour in _plage_jours(debut_dt, fin_dt):
            generateurs = [
                _iterer_canal_retrait(canal, champ, debut_jour, fin_jour, resolution)
                for canal in canaux
            ]
            yield from _fusionner_par_temps(generateurs)
            with _verrou_taches:
                _taches[tache_id]["jours_traites"] += 1

    try:
        if format == "csv":
            with open(chemin, "w", encoding="utf-8", newline="") as f:
                for morceau in _generer_csv(entetes, lignes_avec_suivi()):
                    f.write(morceau)
        else:
            _ecrire_parquet(entetes, lignes_avec_suivi(), chemin)
        with _verrou_taches:
            _taches[tache_id]["statut"] = "termine"
            _taches[tache_id]["jours_traites"] = jours_total
    except Exception as exc:
        with _verrou_taches:
            _taches[tache_id]["statut"] = "erreur"
            _taches[tache_id]["erreur"] = str(exc)
        chemin.unlink(missing_ok=True)


@router.post("/retrait/tache")
def demarrer_tache_retrait(
    canaux: str = Query(...),
    champ: str = Query("valeur_filtree"),
    debut: str = Query(default=None, description=f"Défaut : {DATE_DEBUT_RETRAIT.isoformat()}"),
    fin: str = Query(default=None, description="Défaut : maintenant"),
    resolution: str = Query("heure"),
    format: str = Query("csv"),
    _utilisateur: dict = Depends(utilisateur_courant),
) -> dict:
    """Démarre une génération en arrière-plan, renvoie un identifiant de
    tâche à interroger via GET /retrait/tache/{tache_id}."""
    liste_canaux = [c.strip() for c in canaux.split(",") if c.strip()]
    if not liste_canaux:
        raise HTTPException(status_code=400, detail="Au moins un canal requis.")
    if champ not in _CHAMPS_RETRAIT:
        raise HTTPException(status_code=400, detail=f"Champ invalide : {champ!r}")
    if resolution not in ("brut", "heure", "jour"):
        raise HTTPException(status_code=400, detail=f"Résolution invalide : {resolution!r}")
    if format not in _FORMATS:
        raise HTTPException(status_code=400, detail=f"Format invalide : {format!r}")

    debut_dt = (
        datetime.combine(DATE_DEBUT_RETRAIT, datetime.min.time(), tzinfo=timezone.utc)
        if not debut
        else datetime.fromisoformat(debut.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
    )
    fin_dt = (
        datetime.now(timezone.utc)
        if not fin
        else datetime.fromisoformat(fin.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
    )
    if fin_dt <= debut_dt:
        raise HTTPException(status_code=400, detail="La fin doit être après le début.")

    config.EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    tache_id = uuid.uuid4().hex
    jours_total = max(1, (fin_dt.date() - debut_dt.date()).days + 1)
    with _verrou_taches:
        _taches[tache_id] = {
            "statut": "en_cours",
            "jours_traites": 0,
            "jours_total": jours_total,
            "format": format,
        }
    fil = threading.Thread(
        target=_executer_tache_retrait,
        args=(tache_id, liste_canaux, champ, debut_dt, fin_dt, resolution, format),
        daemon=True,
    )
    fil.start()
    return {"tache_id": tache_id}


@router.get("/retrait/tache/{tache_id}")
def etat_tache_retrait(tache_id: str, _utilisateur: dict = Depends(utilisateur_courant)) -> dict:
    """Avancement d'une tâche d'export retrait démarrée via POST /retrait/tache."""
    with _verrou_taches:
        tache = _taches.get(tache_id)
    if not tache:
        raise HTTPException(status_code=404, detail="Tâche inconnue.")
    return tache


@router.get("/retrait/tache/{tache_id}/telecharger")
def telecharger_tache_retrait(tache_id: str, _utilisateur: dict = Depends(utilisateur_courant)):
    """Télécharge le fichier d'une tâche d'export retrait terminée."""
    with _verrou_taches:
        tache = _taches.get(tache_id)
    if not tache:
        raise HTTPException(status_code=404, detail="Tâche inconnue.")
    if tache["statut"] != "termine":
        raise HTTPException(
            status_code=409, detail=f"Tâche non prête (statut : {tache['statut']})."
        )
    chemin = config.EXPORTS_DIR / f"{tache_id}.{tache['format']}"
    if not chemin.exists():
        raise HTTPException(status_code=404, detail="Fichier introuvable (déjà téléchargé ?).")
    nom = f"retrait_export.{tache['format']}"
    return FileResponse(chemin, media_type="application/octet-stream", filename=nom)


# ===========================================================================
# Export HR/T — volume négligeable (dizaines de milliers de points au
# total, cf. section 32/33), pas besoin de la même refonte : une seule
# requête reste sûre, contrairement au retrait.
# ===========================================================================


def _valeurs_hr_t(
    mur: str | None,
    couche: str | None,
    position: str | None,
    champs: list[str],
    debut: str,
    fin: str,
) -> dict:
    filtres = [f'r._measurement == "{MESURE_CAPTEURS}"']
    filtres.append("(" + " or ".join(f'r._field == "{c}"' for c in champs) + ")")
    if mur:
        filtres.append(f'r.nom_mur == "{flux_escape(mur)}"')
    if couche:
        filtres.append(f'r.nom_couche == "{flux_escape(couche)}"')
    if position:
        filtres.append(f'r.position == "{flux_escape(position)}"')
    clause = "\n  |> filter(fn: (r) => " + ")\n  |> filter(fn: (r) => ".join(filtres) + ")"
    flux = (
        f'from(bucket: "{config.INFLUX_BUCKET}")\n  |> range(start: {debut}, stop: {fin}){clause}'
    )
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Requête InfluxDB échouée : {exc}") from exc

    # Regroupement en Python par (capteur, horodatage) — évite tout pivot()
    # Flux, déjà source d'un bug de perte de tags cette session (section 34).
    par_cle: dict[tuple, dict] = {}
    for table in tables:
        for record in table.records:
            valeurs = record.values
            cle = (valeurs.get("adresse_mac", ""), record.get_time())
            entree = par_cle.setdefault(
                cle,
                {
                    "nom_capteur": valeurs.get("nom_capteur", ""),
                    "nom_mur": valeurs.get("nom_mur", ""),
                    "nom_couche": valeurs.get("nom_couche", ""),
                    "position": valeurs.get("position", ""),
                },
            )
            entree[record.get_field()] = record.get_value()
    return par_cle


@router.get("/hr_t")
def exporter_hr_t(
    mur: str | None = None,
    couche: str | None = None,
    position: str | None = None,
    champs: str = Query("temperature,humidite,point_de_rosee"),
    debut: str = Query(...),
    fin: str = Query(...),
    format: str = Query("csv", description="csv | parquet"),
    _utilisateur: dict = Depends(utilisateur_courant),
):
    """Export "long" : une ligne par capteur/horodatage — contrairement au
    retrait, les capteurs BLE ne partagent pas de grille temporelle commune,
    un format large serait creux et peu lisible."""
    liste_champs = [c.strip() for c in champs.split(",") if c.strip()]
    if not liste_champs:
        raise HTTPException(status_code=400, detail="Au moins une grandeur requise.")
    if format not in _FORMATS:
        raise HTTPException(status_code=400, detail=f"Format invalide : {format!r}")
    debut_iso, fin_iso = _valider_bornes(debut, fin, "hr_t")
    par_cle = _valeurs_hr_t(mur, couche, position, liste_champs, debut_iso, fin_iso)

    entetes = ["temps", "capteur", "mur", "couche", "position"] + liste_champs
    lignes = (
        [t, entree["nom_capteur"], entree["nom_mur"], entree["nom_couche"], entree["position"]]
        + [entree.get(c) for c in liste_champs]
        for (_mac, t), entree in sorted(par_cle.items(), key=lambda kv: kv[0][1])
    )

    if format == "csv":
        return StreamingResponse(
            _generer_csv(entetes, lignes),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="hr_t_export.csv"'},
        )
    chemin = config.EXPORTS_DIR / f"_tmp_{uuid.uuid4().hex}.parquet"
    config.EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    _ecrire_parquet(entetes, lignes, chemin)
    return _reponse_fichier(chemin, "hr_t_export.parquet")


# ===========================================================================
# Export teneur en eau — volume négligeable (relevés ponctuels manuels).
# ===========================================================================


@router.get("/teneur_eau")
def exporter_teneur_eau(
    debut: str | None = None,
    fin: str | None = None,
    format: str = Query("csv", description="csv | parquet"),
    _utilisateur: dict = Depends(utilisateur_courant),
):
    """Export "long", volume négligeable — pas de contrainte de période particulière."""
    if format not in _FORMATS:
        raise HTTPException(status_code=400, detail=f"Format invalide : {format!r}")
    debut_iso, fin_iso = _valider_bornes(debut, fin, "teneur_eau")
    flux = f"""
from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: {debut_iso}, stop: {fin_iso})
  |> filter(fn: (r) => r._measurement == "{MESURE_TENEUR_EAU}")
  |> filter(fn: (r) => r._field == "teneur_eau_pourcent" or r._field == "commentaire")
"""
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Requête InfluxDB échouée : {exc}") from exc

    par_cle: dict[tuple, dict] = {}
    for table in tables:
        for record in table.records:
            valeurs = record.values
            cle = (valeurs.get("mur", ""), valeurs.get("couche", ""), record.get_time())
            entree = par_cle.setdefault(cle, {})
            entree[record.get_field()] = record.get_value()

    entetes = ["temps", "mur", "couche", "teneur_eau_pourcent", "commentaire"]
    lignes = (
        [t, mur, couche, entree.get("teneur_eau_pourcent"), entree.get("commentaire")]
        for (mur, couche, t), entree in sorted(par_cle.items(), key=lambda kv: kv[0][2])
    )

    if format == "csv":
        return StreamingResponse(
            _generer_csv(entetes, lignes),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="teneur_eau_export.csv"'},
        )
    chemin = config.EXPORTS_DIR / f"_tmp_{uuid.uuid4().hex}.parquet"
    config.EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    _ecrire_parquet(entetes, lignes, chemin)
    return _reponse_fichier(chemin, "teneur_eau_export.parquet")
