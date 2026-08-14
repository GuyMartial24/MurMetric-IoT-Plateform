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

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from .. import config
from ..auth import utilisateur_courant

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


@router.put("/hr_t/{mac}")
def modifier_capteur_hr_t(
    mac: str, modification: ModificationCapteur, _utilisateur: dict = Depends(utilisateur_courant)
) -> dict:
    """Édite l'étiquetage d'un capteur HR/T existant (réservé aux utilisateurs connectés)."""
    return _modifier_entree(config.CAPTEURS_JSON, mac, modification)


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
