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

import hmac
import json
import threading
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

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


@router.get("/hr_t/dernieres_mesures")
def dernieres_mesures_hr_t() -> dict:
    """Dernière température/humidité connue par capteur HR/T actif
    (`ingestion: true`), public — une seule requête InfluxDB groupée pour
    toute la page Capteurs (cf. Capteurs.jsx, colonne "Dernière mesure",
    26/08/2026) plutôt qu'une requête par ligne. Capteurs `ingestion: false`
    volontairement absents du résultat : jamais aucune mesure écrite pour
    eux (cf. kafka_consumer_influx.py), pas la peine de les interroger.

    Ajoute aussi `intervalle_observe_s` pour les capteurs ELA (27/08/2026) :
    contrairement à Blue Maestro (lint_max_confirme_s, confirmé par GATT),
    ELA n'a aucun canal de confirmation — sa Measurement Period est figée en
    NFC et invisible depuis le BLE (cf. note_frequence_nfc). On calcule donc
    un intervalle *observé*, dérivé du délai réel entre les deux dernières
    valeurs température écrites en base — fiable car le filtre anti-doublon
    du Pi (ingestion_capteurs_bluetooth.py) garantit que deux points
    consécutifs stockés correspondent à deux mesures réellement distinctes,
    pas juste deux trames advertising répétant la même valeur. Reflète la
    cadence effective des trames reçues, pas forcément le Measurement Period
    exact si Advertising Period diverge en configuration NFC — best-effort,
    n'empêche jamais la réponse principale de partir si cette partie échoue."""
    donnees = _lire_json(config.CAPTEURS_JSON)
    macs_actifs = [
        k for k, v in donnees.items() if not k.startswith("_") and v.get("ingestion")
    ]
    if not macs_actifs:
        return {}

    filtre_macs = " or ".join(
        f'r.adresse_mac == "{flux_escape(m)}"' for m in macs_actifs
    )
    flux = f"""
from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: -90d)
  |> filter(fn: (r) => r._measurement == "{MESURE_CAPTEURS}")
  |> filter(fn: (r) => r._field == "temperature" or r._field == "humidite")
  |> filter(fn: (r) => {filtre_macs})
  |> group(columns: ["adresse_mac", "_field"])
  |> last()
"""
    try:
        tables = query_api().query(flux, org=config.INFLUX_ORG)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Requête InfluxDB échouée : {exc}"
        ) from exc

    resultat: dict[str, dict] = {}
    for table in tables:
        for record in table.records:
            mac = record.values.get("adresse_mac")
            entree = resultat.setdefault(mac, {})
            entree[record.get_field()] = record.get_value()
            t = record.get_time()
            if t and (entree.get("heure") is None or t > entree["heure"]):
                entree["heure"] = t
    for entree in resultat.values():
        if entree.get("heure") is not None:
            entree["heure"] = entree["heure"].isoformat()

    macs_ela = [m for m in macs_actifs if donnees[m].get("famille_capteur") == "ela"]
    if macs_ela:
        filtre_macs_ela = " or ".join(
            f'r.adresse_mac == "{flux_escape(m)}"' for m in macs_ela
        )
        flux_intervalle = f"""
from(bucket: "{config.INFLUX_BUCKET}")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "{MESURE_CAPTEURS}")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => {filtre_macs_ela})
  |> group(columns: ["adresse_mac"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 2)
"""
        try:
            for table in query_api().query(flux_intervalle, org=config.INFLUX_ORG):
                horodatages = [r.get_time() for r in table.records]
                if len(horodatages) == 2:
                    mac = table.records[0].values.get("adresse_mac")
                    delta = abs((horodatages[0] - horodatages[1]).total_seconds())
                    resultat.setdefault(mac, {})["intervalle_observe_s"] = delta
        except Exception as exc:  # noqa: BLE001 - best-effort, cf. docstring
            print(f"Attention : calcul intervalle_observe_s ELA échoué : {exc}")

    return resultat


# ===========================================================================
# Édition par un utilisateur connecté — champs d'identité/étiquetage
# seulement (jamais les champs techniques BLE lint_*/mac_complete_connue/
# famille_capteur : ceux-là restent la propriété locale de configure_capteurs.py
# sur le Pi, cf. logique_projet.md section 32, sans rapport avec le
# split-brain mur/couche/position qui a motivé ce chantier).
# ===========================================================================


class ModificationCapteur(BaseModel):
    """Champs d'identité/étiquetage modifiables par un utilisateur connecté.

    lint_cible_s (26/08/2026) déroge volontairement à la règle "lint_* est
    la propriété locale de configure_capteurs.py" (cf. commentaire plus haut) :
    c'est la SEULE partie de la config lint qu'un humain doit pouvoir piloter
    (l'intervalle de mesure souhaité) — lint_configure/lint_max_confirme_s
    restent en lecture seule, écrits uniquement par le Pi une fois la
    commande GATT réellement confirmée. Bornes 1-86400s : plage matérielle
    documentée (configure_capteurs.py, Disc Maxi/Mini).

    note_frequence_nfc (27/08/2026) : pense-bête texte libre pour les
    capteurs ELA, qui n'ont pas d'équivalent lint_cible_s — leur Measurement
    Period est figée en NFC (logiciel "Device Manager" ELA, contact
    physique), sans aucun canal de configuration ou de confirmation à
    distance (cf. docs.elainnovation.com, sensors-configurations/
    general-case : "La configuration doit être écrite dans le tag via NFC
    pour être appliquée"). Purement déclaratif, jamais vérifié ni appliqué
    par un script — à ne pas confondre avec lint_cible_s/lint_max_confirme_s
    qui pilotent réellement le capteur."""

    nom: str | None = None
    emplacement: str | None = None
    nom_mur: str | None = None
    nom_couche: str | None = None
    position: str | None = None
    prestation: str | None = None
    categorie_rd: str | None = None
    ingestion: bool | None = None
    lint_cible_s: int | None = Field(None, ge=1, le=86400)
    note_frequence_nfc: str | None = Field(None, max_length=200)


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


def _reetiqueter_mesures_capteurs(
    mac: str, tags_avant: dict, tags_apres: dict, mac_apres: str | None = None
) -> int:
    """Réécrit tout l'historique InfluxDB (mesures_capteurs) d'un capteur avec
    les nouveaux tags d'étiquetage, par delete-by-predicate + réécriture (même
    principe que teneur_eau.corriger(), cf. logique_projet.md section 33 pour
    le bug de doublon que ce principe corrige quand il est appliqué correctement).

    mac_apres : si fourni, réécrit aussi le tag adresse_mac lui-même — fusion
    d'une clé provisoire de backfill vers sa MAC complète (cf.
    reconcilier_capteur_hr_t / enregistrer_capteur_hr_t, 26/08/2026) ; sinon
    le tag adresse_mac reste `mac` comme avant.

    Sans effet si ni les tags ni la MAC n'ont changé (ingestion/prestation ne
    sont pas des tags InfluxDB pour cette mesure — inutile de réécrire).

    Volume négligeable pour mesures_capteurs (dizaines de milliers de points au
    total, largement moins par capteur) — SANS commune mesure avec
    mesures_dewesoft (1,5 milliard de points), jamais tenté ici après les
    incidents répétés du 17-18/08/2026 sur cette dernière mesure."""
    mac_ecriture = mac_apres or mac
    if tags_avant == tags_apres and mac_ecriture == mac:
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
    tags_ligne = ",".join(
        f"{cle}={echap_tag(valeur)}" for cle, valeur in tags_apres.items()
    )

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
            f"{MESURE_CAPTEURS},adresse_mac={echap_tag(mac_ecriture)},{tags_ligne} "
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
    mac: str,
    modification: ModificationCapteur,
    _utilisateur: dict = Depends(utilisateur_courant),
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


class ReconciliationCapteur(BaseModel):
    """MAC complète à fusionner avec une entrée provisoire de backfill."""

    nouvelle_mac: str


@router.post("/hr_t/{mac_provisoire}/reconcilier")
def reconcilier_capteur_hr_t(
    mac_provisoire: str,
    reconciliation: ReconciliationCapteur,
    _utilisateur: dict = Depends(utilisateur_courant),
) -> dict:
    """Fusionne une entrée provisoire de backfill (clé = 4 premiers octets de
    la MAC, `mac_complete_connue: false`, cf. logique_projet.md section 30)
    avec l'entrée MAC complète correspondante, une fois cette dernière
    détectée en direct par le Pi. Étiquetage (mur/couche/position/nom/
    ingestion) conservé depuis l'entrée provisoire ; télémétrie (dernière
    détection/RSSI/batterie) conservée depuis l'entrée MAC complète.
    Réétiquette rétroactivement l'historique InfluxDB de la clé provisoire
    vers la MAC complète, puis supprime l'entrée provisoire. Réservé à un
    utilisateur connecté — fusion volontaire, jamais automatique pour une
    action qui supprime une entrée du registre (cf. enregistrer_capteur_hr_t
    pour l'équivalent automatique appliqué aux futures détections)."""
    nouvelle_mac = reconciliation.nouvelle_mac.upper()
    with _verrou:
        donnees = _lire_json(config.CAPTEURS_JSON)
        if mac_provisoire not in donnees:
            raise HTTPException(
                status_code=404, detail=f"Entrée provisoire inconnue : {mac_provisoire}"
            )
        cles_completes = {
            k.upper(): k
            for k in donnees
            if k != mac_provisoire and not k.startswith("_")
        }
        if nouvelle_mac not in cles_completes:
            raise HTTPException(
                status_code=404,
                detail=f"Entrée MAC complète inconnue : {reconciliation.nouvelle_mac}",
            )
        if nouvelle_mac.replace(":", "")[:8] != mac_provisoire.upper():
            raise HTTPException(
                status_code=400,
                detail=(
                    "La MAC complète ne correspond pas à cette clé provisoire "
                    "(4 premiers octets différents) — fusion refusée."
                ),
            )
        cle_nouvelle = cles_completes[nouvelle_mac]
        entree_provisoire = donnees[mac_provisoire]
        entree_complete = donnees[cle_nouvelle]

        tags_avant = _tags_capteur(entree_provisoire)
        fusion = dict(entree_provisoire)
        # famille_capteur pris depuis la détection live, pas le backfill —
        # même raisonnement que enregistrer_capteur_hr_t (décodée en direct
        # depuis le paquet BLE, plus fiable ; souvent absente des entrées de
        # backfill d'origine, cf. section 30, antérieures à ce champ).
        for champ in (
            "derniere_detection",
            "dernier_rssi",
            "derniere_batterie",
            "famille_capteur",
        ):
            if champ in entree_complete:
                fusion[champ] = entree_complete[champ]
        fusion["mac"] = cle_nouvelle
        fusion["mac_complete_connue"] = True

        del donnees[mac_provisoire]
        donnees[cle_nouvelle] = fusion
        _ecrire_json(config.CAPTEURS_JSON, donnees)

    nb_points = _reetiqueter_mesures_capteurs(
        mac_provisoire, tags_avant, _tags_capteur(fusion), mac_apres=cle_nouvelle
    )
    return {"entree": fusion, "points_reetiquetes": nb_points}


@router.put("/retrait/{canal}")
def modifier_capteur_retrait(
    canal: str,
    modification: ModificationCapteur,
    _utilisateur: dict = Depends(utilisateur_courant),
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
    pas 401/403 — ne pas révéler l'existence de la route à un appelant non autorisé).

    Comparaison en temps constant (31/08/2026, durcissement — revue de
    sécurité) : `!=` sur des chaînes s'arrête au premier caractère différent,
    ce qui fuit une information temporelle exploitable en théorie pour
    reconstituer la clé caractère par caractère (timing attack). Peu
    praticable à distance (le bruit réseau noie généralement l'écart), mais
    `hmac.compare_digest` coûte rien à utiliser systématiquement pour un
    secret comparé côté serveur."""
    if (
        not config.INGESTION_API_KEY
        or not x_ingestion_key
        or not hmac.compare_digest(x_ingestion_key, config.INGESTION_API_KEY)
    ):
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
    """Crée l'entrée d'un capteur BLE inconnu (ingestion=false) — idempotent.

    Si une entrée provisoire de backfill (clé = 4 premiers octets de la MAC,
    cf. section 30) correspond à cette MAC, fusionne directement avec elle
    (même règles que reconcilier_capteur_hr_t : étiquetage de l'entrée
    provisoire conservé, `famille_capteur` pris depuis la détection live —
    plus fiable qu'une valeur de backfill puisque décodée en direct depuis le
    paquet BLE) plutôt que de créer un doublon non étiqueté — referme
    structurellement l'écart entre backfill et détection live pour toute
    future détection (cf. logique_projet.md, chantier MAC provisoires du
    26/08/2026 ; reconcilier_capteur_hr_t reste nécessaire pour les
    correspondances déjà détectées avant ce correctif)."""
    a_reetiqueter: tuple[str, dict, dict, str] | None = None
    with _verrou:
        donnees = _lire_json(config.CAPTEURS_JSON)
        mac = enregistrement.mac.upper()
        macs_existantes = {k.upper(): k for k in donnees if not k.startswith("_")}
        if mac in macs_existantes:
            return donnees[macs_existantes[mac]]

        prefixe = mac.replace(":", "")[:8]
        if prefixe in donnees:
            entree_provisoire = donnees[prefixe]
            tags_avant = _tags_capteur(entree_provisoire)
            entree = dict(entree_provisoire)
            entree["mac"] = enregistrement.mac
            entree["famille_capteur"] = enregistrement.famille_capteur
            entree["mac_complete_connue"] = True
            del donnees[prefixe]
            donnees[enregistrement.mac] = entree
            _ecrire_json(config.CAPTEURS_JSON, donnees)
            a_reetiqueter = (
                prefixe,
                tags_avant,
                _tags_capteur(entree),
                enregistrement.mac,
            )
        else:
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

    if a_reetiqueter is not None:
        mac_avant, tags_avant, tags_apres, mac_apres = a_reetiqueter
        _reetiqueter_mesures_capteurs(
            mac_avant, tags_avant, tags_apres, mac_apres=mac_apres
        )
    return entree


class TelemetrieCapteur(BaseModel):
    """Télémétrie ponctuelle (santé radio/batterie) envoyée par le script
    d'ingestion à chaque détection — distincte d'une mesure physique."""

    rssi: int | None = None
    batterie: int | None = None


@router.post("/hr_t/{mac}/telemetrie", dependencies=[Depends(_verifier_cle_ingestion)])
def mettre_a_jour_telemetrie_hr_t(mac: str, telemetrie: TelemetrieCapteur) -> dict:
    """Met à jour la dernière détection/RSSI/batterie d'un capteur BLE déjà
    enregistré — appelée par le script d'ingestion indépendamment du flag
    `ingestion` (utile pour surveiller un capteur pas encore activé, ou
    anticiper une perte de signal avant qu'elle ne se produise). N'écrit
    jamais dans InfluxDB (pas une mesure physique) et ne déclenche aucun
    réétiquetage — à distinguer de modifier_capteur_hr_t (édition humaine
    de l'identité/l'étiquetage)."""
    with _verrou:
        donnees = _lire_json(config.CAPTEURS_JSON)
        macs_existantes = {k.upper(): k for k in donnees if not k.startswith("_")}
        cle = macs_existantes.get(mac.upper())
        if cle is None:
            raise HTTPException(status_code=404, detail=f"Capteur inconnu : {mac}")
        entree = donnees[cle]
        entree["derniere_detection"] = datetime.now(timezone.utc).isoformat()
        if telemetrie.rssi is not None:
            entree["dernier_rssi"] = telemetrie.rssi
        if telemetrie.batterie is not None:
            entree["derniere_batterie"] = telemetrie.batterie
        _ecrire_json(config.CAPTEURS_JSON, donnees)
        return entree


class ConfirmationLintCapteur(BaseModel):
    """Intervalle de log réellement confirmé par un capteur Blue Maestro,
    après une commande GATT (setlog~/lint) et lecture de la trame
    advertising suivante."""

    lint_max_confirme_s: float


@router.post(
    "/hr_t/{mac}/lint-confirme", dependencies=[Depends(_verifier_cle_ingestion)]
)
def confirmer_lint_hr_t(mac: str, confirmation: ConfirmationLintCapteur) -> dict:
    """Enregistre la confirmation d'intervalle de log poussée par le Pi
    (configure_capteurs.py) après une reconfiguration GATT réussie
    (27/08/2026). Avant cet endpoint, lint_configure/lint_max_confirme_s
    n'étaient écrits que dans le capteurs.json LOCAL du Pi (configure_
    capteurs.py n'a pas d'accès réseau) — jamais remontés vers la webapp,
    qui ne pouvait donc jamais refléter une reconfiguration pourtant
    réussie (colonne "Intervalle mesure" bloquée indéfiniment sur "en
    attente"). Même mécanisme que mettre_a_jour_telemetrie_hr_t : clé
    d'ingestion, écrit uniquement les champs techniques concernés."""
    with _verrou:
        donnees = _lire_json(config.CAPTEURS_JSON)
        macs_existantes = {k.upper(): k for k in donnees if not k.startswith("_")}
        cle = macs_existantes.get(mac.upper())
        if cle is None:
            raise HTTPException(status_code=404, detail=f"Capteur inconnu : {mac}")
        entree = donnees[cle]
        entree["lint_configure"] = True
        entree["lint_max_confirme_s"] = confirmation.lint_max_confirme_s
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
