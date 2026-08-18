"""Registre capteurs — source unique de vérité (chantier "source unique",
section 32, 13/08/2026). Avant ce chantier, capteurs.json/capteurs_retrait.json
existaient en trois copies non synchronisées (dépôt git, PC Amiens/Pi, image
webapp) ; seules les copies PC Amiens/Pi comptaient réellement pour
l'étiquetage des mesures en direct — éditer depuis la webapp était cosmétique
et sans effet. Désormais ces fichiers vivent sur le volume persistant de la
webapp (cf. config.py) et sont la source de vérité :
- Lecture (GET) : publique, sans auth — utilisée par l'UI ET par
  ingestion_dewesoft_dxd.py (PC Amiens) / ingestion_capteurs_bluetooth.py
  (Pi), qui interrogent désormais cette API au lieu de leur copie locale.
- Édition d'un champ existant (PUT) : réservée à un utilisateur connecté
  (JWT) — c'est un humain qui étiquette mur/couche/position, pas un script.
- Enregistrement d'un canal/MAC inconnu (POST .../enregistrer) : réservée aux
  scripts d'ingestion via une clé partagée (INGESTION_API_KEY), pas de
  session utilisateur possible pour un process qui tourne sans surveillance.
"""

import json
import threading
from datetime import timedelta

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from .. import config
from ..auth import utilisateur_courant
from ..influx import (
    MESURE_CAPTEURS,
    delete_points,
    echap_field_str,
    echap_tag,
    flux_escape,
    query_api,
    write_point,
)

router = APIRouter(prefix="/api/capteurs", tags=["capteurs"])

_verrou = threading.Lock()


def _lire_json(chemin) -> dict:
    if not chemin.exists():
        raise HTTPException(status_code=500, detail=f"Fichier introuvable : {chemin}")
    # utf-8-sig : capteurs.json a porté un BOM par le passé (cf. section 30),
    # tolérant même si le fichier actuel n'en a plus.
    with open(chemin, encoding="utf-8-sig") as f:
        return json.load(f)


def _ecrire_json(chemin, donnees: dict) -> None:
    chemin.parent.mkdir(parents=True, exist_ok=True)
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(donnees, f, ensure_ascii=False, indent=2)


@router.get("/hr_t")
def capteurs_hr_t() -> dict:
    """Registre complet des capteurs BLE (humidité/température), public."""
    return _lire_json(config.CAPTEURS_JSON)


@router.get("/retrait")
def capteurs_retrait() -> dict:
    """Registre complet des canaux DeweSoft (retrait), public."""
    return _lire_json(config.CAPTEURS_RETRAIT_JSON)


# ===========================================================================
# Édition par un utilisateur connecté — champs d'identité/étiquetage
# seulement (jamais les champs techniques BLE lint_*/mac_complete_connue/
# famille_capteur : ceux-là restent la propriété locale de configure_capteurs.py
# sur le Pi, cf. logique_projet.md section 32, sans rapport avec le
# split-brain mur/couche/position qui a motivé ce chantier).
# ===========================================================================


class ModificationCapteur(BaseModel):
    """Champs d'identité/étiquetage modifiables par un utilisateur connecté."""

    nom: str | None = None
    emplacement: str | None = None
    nom_mur: str | None = None
    nom_couche: str | None = None
    position: str | None = None
    prestation: str | None = None
    categorie_rd: str | None = None
    ingestion: bool | None = None


_ALIAS_CHAMPS = {"categorie_rd": "categorie R&D"}


def _modifier_entree(chemin, cle: str, modification: ModificationCapteur) -> dict:
    """Applique les champs fournis (exclude_unset) à l'entrée `cle` du registre `chemin`."""
    with _verrou:
        donnees = _lire_json(chemin)
        if cle not in donnees:
            raise HTTPException(status_code=404, detail=f"Entrée inconnue : {cle}")
        entree = donnees[cle]
        for champ, valeur in modification.model_dump(exclude_unset=True).items():
            entree[_ALIAS_CHAMPS.get(champ, champ)] = valeur
        _ecrire_json(chemin, donnees)
        return entree


def _tags_capteur(entree: dict) -> dict[str, str]:
    """Tags InfluxDB tels que kafka_consumer_influx.py les construirait pour ce
    capteur (mêmes valeurs de repli : "Non défini"/"Inconnu") — sert à détecter
    si un réétiquetage rétroactif est nécessaire après une modification."""
    return {
        "emplacement": str(entree.get("emplacement") or "Non défini"),
        "nom_capteur": str(entree.get("nom") or "Inconnu"),
        "nom_couche": str(entree.get("nom_couche") or "Non défini"),
        "nom_mur": str(entree.get("nom_mur") or "Non défini"),
        "position": str(entree.get("position") or "Non défini"),
        "rd": str(entree.get("categorie R&D") or "Non défini"),
    }


def _reetiqueter_mesures_capteurs(mac: str, tags_avant: dict, tags_apres: dict) -> int:
    """Réécrit tout l'historique InfluxDB (mesures_capteurs) d'un capteur avec
    les nouveaux tags d'étiquetage, par delete-by-predicate + réécriture (même
    principe que teneur_eau.corriger(), cf. logique_projet.md section 33 pour
    le bug de doublon que ce principe corrige quand il est appliqué correctement).

    Sans effet si aucun tag InfluxDB pertinent n'a changé (ingestion/prestation
    ne sont pas des tags InfluxDB pour cette mesure — inutile de réécrire).

    Volume négligeable pour mesures_capteurs (dizaines de milliers de points au
    total, largement moins par capteur) — SANS commune mesure avec
    mesures_dewesoft (1,5 milliard de points), jamais tenté ici après les
    incidents répétés du 17-18/08/2026 sur cette dernière mesure."""
    if tags_avant == tags_apres:
        return 0

    flux = f"""
from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: 2024-01-01T00:00:00Z)
  |> filter(fn: (r) => r._measurement == "{MESURE_CAPTEURS}")
  |> filter(fn: (r) => r.adresse_mac == "{flux_escape(mac)}")
"""
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Lecture InfluxDB (réétiquetage) échouée : {exc}"
        ) from exc

    points_par_time: dict = {}
    for table in tables:
        for record in table.records:
            points_par_time.setdefault(record.get_time(), {})[
                record.get_field()
            ] = record.get_value()

    if not points_par_time:
        return 0

    horodatages = sorted(points_par_time)
    marge = timedelta(seconds=1)
    tags_ligne = ",".join(f"{cle}={echap_tag(valeur)}" for cle, valeur in tags_apres.items())

    lignes = []
    for t, champs in points_par_time.items():
        parts_champs = []
        for nom_champ, valeur in champs.items():
            if isinstance(valeur, bool):
                parts_champs.append(f"{nom_champ}={'true' if valeur else 'false'}")
            elif isinstance(valeur, int):
                parts_champs.append(f"{nom_champ}={valeur}i")
            elif isinstance(valeur, float):
                parts_champs.append(f"{nom_champ}={valeur}")
            else:
                parts_champs.append(f'{nom_champ}="{echap_field_str(str(valeur))}"')
        ts_ns = int(t.timestamp() * 1_000_000_000)
        lignes.append(
            f"{MESURE_CAPTEURS},adresse_mac={echap_tag(mac)},{tags_ligne} "
            f"{','.join(parts_champs)} {ts_ns}"
        )

    predicat = f'_measurement="{MESURE_CAPTEURS}" AND adresse_mac="{flux_escape(mac)}"'
    try:
        delete_points(predicat, horodatages[0] - marge, horodatages[-1] + marge)
        for i in range(0, len(lignes), 500):
            write_point("\n".join(lignes[i : i + 500]))
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                f"Réétiquetage InfluxDB échoué après lecture de {len(points_par_time)} "
                f"points ({exc}) — le registre est déjà mis à jour, l'historique InfluxDB "
                "peut être incohérent ; relancer la même modification pour réessayer."
            ),
        ) from exc
    return len(points_par_time)


@router.put("/hr_t/{mac}")
def modifier_capteur_hr_t(
    mac: str, modification: ModificationCapteur, _utilisateur: dict = Depends(utilisateur_courant)
) -> dict:
    """Édite l'étiquetage d'un capteur HR/T existant (réservé aux utilisateurs
    connectés). Réétiquette aussi rétroactivement tout l'historique InfluxDB de
    ce capteur si un champ correspondant à un tag InfluxDB a changé — sans quoi
    l'ancien et le nouvel étiquetage cohabiteraient indéfiniment comme deux
    entités distinctes dans Grafana/la webapp (cf. _reetiqueter_mesures_capteurs)."""
    donnees_avant = _lire_json(config.CAPTEURS_JSON)
    if mac not in donnees_avant:
        raise HTTPException(status_code=404, detail=f"Entrée inconnue : {mac}")
    tags_avant = _tags_capteur(donnees_avant[mac])
    entree = _modifier_entree(config.CAPTEURS_JSON, mac, modification)
    _reetiqueter_mesures_capteurs(mac, tags_avant, _tags_capteur(entree))
    return entree


@router.put("/retrait/{canal}")
def modifier_capteur_retrait(
    canal: str, modification: ModificationCapteur, _utilisateur: dict = Depends(utilisateur_courant)
) -> dict:
    """Édite l'étiquetage d'un canal retrait existant (réservé aux utilisateurs connectés)."""
    return _modifier_entree(config.CAPTEURS_RETRAIT_JSON, canal, modification)


# ===========================================================================
# Enregistrement d'un canal/MAC inconnu par un script d'ingestion — miroir
# exact de enregistrer_capteur_si_inconnu()/enregistrer_canal_si_inconnu()
# (auparavant des écritures locales dans ingestion_capteurs_bluetooth.py/
# ingestion_dewesoft_dxd.py) : entrée vide créée avec ingestion=false, aucune
# mesure n'est donc jamais publiée silencieusement pour un capteur/canal
# encore non étiqueté. Idempotent : no-op si déjà connu.
# ===========================================================================


def _verifier_cle_ingestion(x_ingestion_key: str | None = Header(default=None)) -> None:
    """Dépendance FastAPI : exige l'en-tête X-Ingestion-Key (404 générique si absent/faux,
    pas 401/403 — ne pas révéler l'existence de la route à un appelant non autorisé)."""
    if not config.INGESTION_API_KEY or x_ingestion_key != config.INGESTION_API_KEY:
        raise HTTPException(status_code=404)


class EnregistrementHrT(BaseModel):
    """Déclaration d'un capteur BLE inconnu par un script d'ingestion."""

    mac: str
    famille_capteur: str = "bluemaestro"


class EnregistrementRetrait(BaseModel):
    """Déclaration d'un canal DeweSoft inconnu par un script d'ingestion."""

    canal: str


@router.post("/hr_t/enregistrer", dependencies=[Depends(_verifier_cle_ingestion)])
def enregistrer_capteur_hr_t(enregistrement: EnregistrementHrT) -> dict:
    """Crée l'entrée d'un capteur BLE inconnu (ingestion=false) — idempotent."""
    with _verrou:
        donnees = _lire_json(config.CAPTEURS_JSON)
        mac = enregistrement.mac.upper()
        macs_existantes = {k.upper(): k for k in donnees if not k.startswith("_")}
        if mac in macs_existantes:
            return donnees[macs_existantes[mac]]
        entree = {
            "mac": enregistrement.mac,
            "famille_capteur": enregistrement.famille_capteur,
            "nom": "",
            "emplacement": "",
            "nom_mur": "",
            "nom_couche": "",
            "position": "",
            "prestation": "",
            "categorie R&D": "",
            "ingestion": False,
        }
        donnees[enregistrement.mac] = entree
        _ecrire_json(config.CAPTEURS_JSON, donnees)
        return entree


@router.post("/retrait/enregistrer", dependencies=[Depends(_verifier_cle_ingestion)])
def enregistrer_capteur_retrait(enregistrement: EnregistrementRetrait) -> dict:
    """Crée l'entrée d'un canal DeweSoft inconnu (ingestion=false) — idempotent."""
    with _verrou:
        donnees = _lire_json(config.CAPTEURS_RETRAIT_JSON)
        if enregistrement.canal in donnees:
            return donnees[enregistrement.canal]
        entree = {
            "canal": enregistrement.canal,
            "nom_mur": "",
            "nom_couche": "",
            "position": "",
            "categorie R&D": "",
            "prestation": "",
            "ingestion": False,
        }
        donnees[enregistrement.canal] = entree
        _ecrire_json(config.CAPTEURS_RETRAIT_JSON, donnees)
        return entree
