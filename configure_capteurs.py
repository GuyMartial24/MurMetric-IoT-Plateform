"""
Configuration active de l'intervalle de log des capteurs BLE Blue Maestro.

Ce script se connecte à chaque capteur Blue Maestro détecté via GATT (connexion
active, pas seulement advertising passif) et envoie la commande appropriée pour
régler l'intervalle de log à sa valeur maximale, prolongeant ainsi la durée de
vie des piles CR2477 (pile bouton, autonomie 4 à 5 ans selon usage).

Protocole GATT utilisé :
    Service  : Nordic UART Service (UUID 6E400001-…)
    Écriture : UUID 6E400002 [write, write-without-response]  — host → device
    Lecture  : UUID 6E400003 [notify]                         — device → host

    Attention : sur le firmware Disc Maxi, ces UUIDs sont INVERSÉS par rapport
    au standard Nordic UART (où 6E400002 = TX notify, 6E400003 = RX write).

Format des commandes (selon génération firmware) :
    v41/42/43 (Disc Maxi moderne) : ``setlog~<secondes>``
    v13/23/27 (Disc Mini legacy)  : ``*lint<secondes>``

Séquence d'exécution :
    Phase 1 — Scan passif 30 s : collecte les objets BLEDevice complets.
    Phase 2 — Connexion GATT  : envoi de la commande d'intervalle.
    Phase 3 — Vérification    : lecture du champ lint dans la prochaine trame
                                 advertising pour confirmer l'application.

Usage :
    python -u configure_capteurs.py
    (lancé automatiquement par start.py et périodiquement par test_ingestion.py)
"""

import asyncio
import json
import os
import re
import sys

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice

# Requis pour afficher les emojis sous Windows (console cp1252 par défaut).
sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Constantes protocole Blue Maestro.
# ---------------------------------------------------------------------------

# Company ID Bluetooth SIG de Blue Maestro Limited.
BLUEMAESTRO_COMPANY_ID = 0x0133

# Versions du protocole Blue Maestro supportées.
VERSIONS_CONNUES = {13, 23, 27, 41, 42, 43}

# Seuil RSSI en dessous duquel on ignore la détection (artefact cache Windows).
RSSI_MIN_VALIDE = -100

# Regex de validation du format MAC BLE.
MAC_REGEX = re.compile(r"^([0-9A-F]{2}:){5}[0-9A-F]{2}$", re.IGNORECASE)

# ---------------------------------------------------------------------------
# UUIDs du Nordic UART Service — Disc Maxi A03 (v42).
#
# IMPORTANT : sur ce firmware, les UUIDs sont INVERSÉS vs standard Nordic :
#   6E400002 → [write, write-without-response] → on ÉCRIT les commandes ici
#   6E400003 → [notify]                        → le device ENVOIE ses réponses ici
# ---------------------------------------------------------------------------
NORDIC_UART_TX = "6E400003-B5A3-F393-E0A9-E50E24DCCA9E"  # device → host
NORDIC_UART_RX = "6E400002-B5A3-F393-E0A9-E50E24DCCA9E"  # host → device

# ---------------------------------------------------------------------------
# Paramètres de configuration.
# ---------------------------------------------------------------------------

# Durée du scan BLE passif pour collecter les BLEDevice (Phase 1).
DUREE_SCAN_INITIAL = 30

# Valeur d'intervalle de log cible en secondes :
#   - Disc Maxi v41/42/43 : 1 s – 86 400 s (24 h), commande setlog~<s>
#   - Disc Mini v13/23/27 : 2 s – 86 400 s (24 h), commande *lint<s>
LINT_CIBLE = 86400

# Chemin vers le fichier de mapping MAC → infos capteur.
CAPTEURS_FILE = os.path.join(os.path.dirname(__file__), "capteurs.json")

# ---------------------------------------------------------------------------
# État global collecté pendant le scan passif.
# Dict MAC → {version, lint, rssi, device}.
# L'objet BLEDevice complet est indispensable pour une connexion GATT fiable
# avec les adresses BLE aléatoires (sinon : "device not found").
# ---------------------------------------------------------------------------
capteurs_detectes: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Helpers JSON.
# ---------------------------------------------------------------------------

def lire_capteurs_json() -> dict:
    """Lire capteurs.json et retourner son contenu (dict vide si absent/corrompu).

    Les clés commençant par ``_`` (ex. ``_schema``) sont filtrées : elles
    servent uniquement de documentation inline et ne représentent pas des
    capteurs réels.

    Returns:
        Dictionnaire MAC → infos (clés ``_*`` exclues), ou {} en cas d'erreur.
    """
    try:
        with open(CAPTEURS_FILE, "r", encoding="utf-8") as f:
            brut = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    # Exclure les clés de métadonnées commençant par '_'.
    return {k: v for k, v in brut.items() if not k.startswith("_")}


def ecrire_capteurs_json(donnees: dict) -> None:
    """Sauvegarder le dictionnaire dans capteurs.json (indentation 2 espaces).

    Args:
        donnees: Dictionnaire complet à persister.
    """
    with open(CAPTEURS_FILE, "w", encoding="utf-8") as f:
        json.dump(donnees, f, indent=2, ensure_ascii=False)


def decoder_lint(payload_bytes: list[int], version: int) -> float | None:
    """Extraire l'intervalle de log depuis les octets bruts du payload advertising.

    Args:
        payload_bytes: Octets du payload après retrait de l'entête fabricant.
        version:       Numéro de version du protocole Blue Maestro.

    Returns:
        Intervalle en secondes (float), ou None si trop peu d'octets.
    """
    if version in (41, 42, 43) and len(payload_bytes) >= 6:
        # uint32 little-endian en déciseconde → secondes.
        raw = (
            payload_bytes[2]
            + (payload_bytes[3] << 8)
            + (payload_bytes[4] << 16)
            + (payload_bytes[5] << 24)
        )
        return raw / 10.0
    if version in (13, 23, 27) and len(payload_bytes) >= 4:
        # uint16 big-endian en secondes directement.
        return float((payload_bytes[2] << 8) + payload_bytes[3])
    return None


# ---------------------------------------------------------------------------
# Phase 1 — Scan passif.
# ---------------------------------------------------------------------------

def callback_scan(device: BLEDevice, advertising_data) -> None:
    """Callback BLE du scan passif — collecter les BLEDevice Blue Maestro.

    L'objet BLEDevice complet est stocké (pas seulement la MAC string) car
    bleak en a besoin pour connecter fiablement un device à adresse aléatoire.
    La dict est mise à jour à chaque réception pour conserver l'objet le plus
    récent, donc le plus susceptible d'aboutir à une connexion GATT.

    Args:
        device:           BLEDevice bleak (adresse MAC + métadonnées BLE).
        advertising_data: Données advertising du paquet reçu.
    """
    mac = device.address.upper()
    rssi = advertising_data.rssi

    if rssi is None or rssi <= RSSI_MIN_VALIDE:
        return
    raw_payload = advertising_data.manufacturer_data
    if BLUEMAESTRO_COMPANY_ID not in raw_payload:
        return
    payload_bytes = list(raw_payload[BLUEMAESTRO_COMPANY_ID])
    version = payload_bytes[0] if payload_bytes else None
    if version not in VERSIONS_CONNUES:
        return

    lint = decoder_lint(payload_bytes, version)
    capteurs_detectes[mac] = {
        "version": version,
        "lint": lint,
        "rssi": rssi,
        "device": device,
    }


# ---------------------------------------------------------------------------
# Phase 2 — Connexion GATT et envoi de la commande.
# ---------------------------------------------------------------------------

async def configurer_capteur_gatt(
    device: BLEDevice,
    lint_cible: int,
    version: int = 42,
) -> dict | None:
    """Se connecter au capteur via GATT et envoyer la commande d'intervalle de log.

    La commande est adaptée à la génération du firmware :
    - v41/42/43 : ``setlog~<lint_cible>``   (réponse attendue : "OK: …")
    - v13/23/27 : ``*lint<lint_cible>\r\n`` (réponse attendue : "Command Recognised")

    La déconnexion du device immédiatement après l'écriture est le comportement
    documenté indiquant que la commande a été prise en compte.

    Args:
        device:      BLEDevice bleak (issu du scan Phase 1).
        lint_cible:  Valeur d'intervalle de log souhaitée en secondes.
        version:     Numéro de version du protocole Blue Maestro.

    Returns:
        dict  ``{"texte": str, "reconnue": bool, "gatt_absent": bool}``
              si la connexion GATT a abouti.
        None  si la connexion a échoué de façon inattendue (timeout réseau,
              device introuvable avant que la commande soit envoyée, etc.).
    """
    mac = device.address.upper()

    if version in (41, 42, 43):
        commande = f"setlog~{lint_cible}".encode("ascii")
    else:
        commande = f"*lint{lint_cible}\r\n".encode("ascii")

    reponse_recue = asyncio.Event()
    fragments: list[str] = []

    def handle_notif(_handle, data: bytes) -> None:
        """Accumuler les fragments de réponse et signaler la fin."""
        texte = data.decode("ascii", errors="ignore")
        fragments.append(texte)
        if (
            "Recognised" in texte
            or "Unknown" in texte
            or texte.startswith("OK")
            or texte.startswith("ERR")
        ):
            reponse_recue.set()

    commande_envoyee = False
    print(f"    🔌 Connexion GATT → {mac} ...")
    try:
        async with BleakClient(device, timeout=15.0) as client:
            if not client.is_connected:
                print("    ❌ Connexion refusée.")
                return None

            # Inventaire des caractéristiques GATT — utile au diagnostic.
            services = client.services
            print("    📋 Services GATT détectés :")
            rx_present = False
            for s in services:
                for c in s.characteristics:
                    props = ", ".join(c.properties)
                    uuid_upper = str(c.uuid).upper()
                    label = ""
                    if uuid_upper == NORDIC_UART_TX.upper():
                        label = " ← TX (réponses capteur)"
                    elif uuid_upper == NORDIC_UART_RX.upper():
                        label = " ← RX (commandes)"
                        rx_present = True
                    print(f"       {c.uuid}  [{props}]{label}")

            if not rx_present:
                print(
                    "    ❌ Caractéristique RX Nordic UART absente "
                    "— GATT non supporté."
                )
                return {"texte": "", "reconnue": False, "gatt_absent": True}

            cmd_str = (
                f"setlog~{lint_cible}"
                if version in (41, 42, 43)
                else f"*lint{lint_cible}"
            )
            print(f"    ✅ Connecté. Envoi : {cmd_str}")

            # Souscription aux notifications TX (optionnelle).
            # Si la caractéristique n'expose pas NOTIFY (cas Disc Maxi), on
            # envoie quand même la commande et on vérifiea en Phase 3.
            notifications_actives = False
            try:
                await client.start_notify(NORDIC_UART_TX, handle_notif)
                notifications_actives = True
            except Exception:
                print(
                    "    ℹ️  Notifications TX non disponibles "
                    "— envoi sans retour texte."
                )

            await client.write_gatt_char(NORDIC_UART_RX, commande, response=False)
            commande_envoyee = True

            if notifications_actives:
                try:
                    await asyncio.wait_for(reponse_recue.wait(), timeout=5.0)
                    print(
                        f"    📨 Réponse capteur : "
                        f"{''.join(fragments).strip()}"
                    )
                except asyncio.TimeoutError:
                    print(
                        "    ⚠️  Pas de réponse dans les délais "
                        "— commande peut-être appliquée."
                    )
                try:
                    await client.stop_notify(NORDIC_UART_TX)
                except Exception:
                    # Le device s'est déjà déconnecté après traitement de la
                    # commande — comportement normal documenté.
                    pass
            else:
                print("    📤 Commande envoyée — résultat vérifié en Phase 3.")

    except Exception as exc:
        exc_msg = str(exc).lower()
        if commande_envoyee and (
            "not connected" in exc_msg or "disconnected" in exc_msg
        ):
            # Déconnexion post-commande : comportement attendu (le firmware
            # coupe la connexion après avoir traité la commande).
            print(
                "    ✅ Device déconnecté après traitement de la commande "
                "(comportement attendu)."
            )
        else:
            print(f"    ❌ Erreur GATT : {exc}")
            return None

    texte_final = "".join(fragments).strip()
    # Commande reconnue si la réponse ne contient pas "Unknown" ni "ERR".
    # En l'absence de réponse (timeout ou pas de notifications), on suppose
    # que la commande a été traitée et on laisse la Phase 3 trancher.
    reconnue = "Unknown" not in texte_final and "ERR" not in texte_final
    return {"texte": texte_final, "reconnue": reconnue, "gatt_absent": False}


# ---------------------------------------------------------------------------
# Phase 3 — Vérification sur la prochaine trame advertising.
# ---------------------------------------------------------------------------

async def verifier_lint_apres_config(
    mac: str,
    timeout_s: float = 30.0,
) -> float | None:
    """Attendre la prochaine trame advertising du capteur et lire son lint.

    Après une commande GATT, le firmware efface ses logs internes et redémarre
    l'advertising (opération pouvant prendre jusqu'à quelques dizaines de
    secondes). Cette fonction lance un scan BLE ciblé et résout le futur dès
    qu'un paquet valide du capteur est reçu.

    Args:
        mac:       Adresse MAC du capteur à surveiller (majuscules).
        timeout_s: Durée max d'attente avant d'abandonner la vérification.

    Returns:
        Valeur lint en secondes si confirmée, None si timeout.
    """
    print(
        f"    ⏳ Attente trame advertising post-config "
        f"(max {int(timeout_s)}s)..."
    )
    boucle = asyncio.get_running_loop()
    future_lint: asyncio.Future = boucle.create_future()

    def cb_verif(device, advertising_data) -> None:
        """Résoudre le futur dès qu'un paquet valide du capteur cible arrive."""
        if device.address.upper() != mac:
            return
        rssi = advertising_data.rssi
        if rssi is None or rssi <= RSSI_MIN_VALIDE:
            return
        raw_payload = advertising_data.manufacturer_data
        if BLUEMAESTRO_COMPANY_ID not in raw_payload:
            return
        payload_bytes = list(raw_payload[BLUEMAESTRO_COMPANY_ID])
        version = payload_bytes[0] if payload_bytes else None
        if version not in VERSIONS_CONNUES:
            return
        lint = decoder_lint(payload_bytes, version)
        if lint is not None and not future_lint.done():
            future_lint.set_result(lint)

    scanner = BleakScanner(detection_callback=cb_verif)
    await scanner.start()
    try:
        lint_confirme = await asyncio.wait_for(future_lint, timeout=timeout_s)
        print(
            f"    ✅ Valeur confirmée depuis trame advertising : "
            f"{lint_confirme} s"
        )
        return lint_confirme
    except asyncio.TimeoutError:
        print(
            "    ⚠️  Capteur non reçu dans les délais "
            "— vérification impossible."
        )
        return None
    finally:
        await scanner.stop()


# ---------------------------------------------------------------------------
# Orchestration principale.
# ---------------------------------------------------------------------------

async def main() -> None:
    """Orchestrer les trois phases de configuration active.

    Phase 1 — Scan passif 30 s, scanner maintenu actif pendant Phase 2.
    Phase 2 — Connexion GATT sur chaque capteur non encore configuré.
    Phase 3 — Vérification de la valeur appliquée sur le paquet advertising.
    """
    print(
        f"🔍 Phase 1 — Scan passif {DUREE_SCAN_INITIAL}s "
        "(collecte des objets BLEDevice)...\n"
    )

    # Le scanner reste actif pendant Phase 2 pour garder les BLEDevice "frais"
    # dans le cache WinRT — indispensable pour les adresses BLE aléatoires.
    scanner = BleakScanner(detection_callback=callback_scan)
    await scanner.start()
    await asyncio.sleep(DUREE_SCAN_INITIAL)

    if not capteurs_detectes:
        await scanner.stop()
        print("❌ Aucun capteur Blue Maestro détecté. Vérifiez la portée BLE.")
        return

    print(f"📋 {len(capteurs_detectes)} capteur(s) détecté(s) :")
    for mac, info in list(capteurs_detectes.items()):
        print(
            f"   {mac}  version={info['version']}  "
            f"lint={info['lint']}s  RSSI={info['rssi']} dBm"
        )

    donnees = lire_capteurs_json()
    print(
        f"\n⚙️  Phase 2 — Configuration *lint{LINT_CIBLE} "
        "(scanner BLE maintenu actif)...\n"
    )

    macs_a_verifier: list[str] = []

    for mac, info in list(capteurs_detectes.items()):
        # Les clés _schema et autres métadonnées JSON sont ignorées.
        entree = donnees.get(mac, {})
        lint_actuel = info["lint"]
        lint_max_connu = entree.get("lint_max_confirme_s")
        device = info["device"]

        print(f"━━━ {mac} ━━━")

        # Skip : déjà marqué comme non configurable lors d'un run précédent.
        if entree.get("lint_gatt_non_supporte"):
            print(
                "  ⛔ Commande *lintX non supportée "
                "(détectée lors d'un run précédent). Ignoré.\n"
            )
            continue
        if entree.get("lint_gatt_absent"):
            print(
                "  ⛔ Service GATT absent "
                "(détecté lors d'un run précédent). Ignoré.\n"
            )
            continue

        # Skip : déjà configuré à la valeur max et toujours à cette valeur.
        if (
            entree.get("lint_configure")
            and lint_max_connu
            and lint_actuel == lint_max_connu
        ):
            print(f"  ✔️  Déjà à la valeur max confirmée ({lint_actuel}s). Ignoré.\n")
            continue

        # Skip avec mise à jour du flag si déjà à la valeur cible.
        if lint_actuel == LINT_CIBLE:
            print(f"  ✔️  Intervalle déjà à {LINT_CIBLE}s. Flag mis à jour.\n")
            entree.setdefault("mac", mac)
            entree.setdefault("nom", "")
            entree.setdefault("emplacement", "")
            entree["lint_configure"] = True
            entree["lint_max_confirme_s"] = LINT_CIBLE
            donnees[mac] = entree
            ecrire_capteurs_json(donnees)
            continue

        print(f"  Intervalle actuel : {lint_actuel}s → cible : {LINT_CIBLE}s")
        reponse = await configurer_capteur_gatt(
            device, LINT_CIBLE, version=info["version"]
        )

        # Cas 1 — Échec transitoire (pas de marquage permanent).
        if reponse is None:
            print(
                "  ⚠️  Connexion GATT échouée (transitoire) "
                "— non marqué comme absent.\n"
            )
            continue

        # Cas 2 — Service GATT réellement absent (Nordic UART non trouvé).
        if reponse.get("gatt_absent"):
            entree.setdefault("mac", mac)
            entree["lint_gatt_absent"] = True
            donnees[mac] = entree
            ecrire_capteurs_json(donnees)
            print("  ❌ Service GATT Nordic UART absent — marqué définitivement.\n")
            continue

        # Cas 3 — Commande non reconnue par le firmware.
        if not reponse["reconnue"]:
            entree.setdefault("mac", mac)
            entree["lint_gatt_non_supporte"] = True
            donnees[mac] = entree
            ecrire_capteurs_json(donnees)
            print(
                f"  ❌ *lintX non supportée ({reponse['texte']}) "
                "— marqué définitivement.\n"
            )
            continue

        # Cas 4 — Commande envoyée : planifier la vérification en Phase 3.
        macs_a_verifier.append(mac)
        print("  ✔️  Commande envoyée. Vérification à venir en Phase 3.\n")

    # Arrêt du scanner Phase 1 avant de lancer les scanners ciblés de Phase 3.
    await scanner.stop()

    if not macs_a_verifier:
        print("✅ Configuration terminée (aucune vérification nécessaire).")
        return

    print(
        f"\n🔎 Phase 3 — Vérification de la valeur appliquée "
        "par le firmware...\n"
    )

    for mac in macs_a_verifier:
        entree = donnees.get(mac, {})
        print(f"━━━ {mac} ━━━")
        lint_confirme = await verifier_lint_apres_config(mac)

        if lint_confirme is not None:
            entree.setdefault("mac", mac)
            entree.setdefault("nom", "")
            entree.setdefault("emplacement", "")
            entree["lint_configure"] = True
            entree["lint_max_confirme_s"] = lint_confirme

            if lint_confirme == LINT_CIBLE:
                print(f"  🎯 Valeur max appliquée : {lint_confirme}s (= cible envoyée)")
            else:
                print(
                    f"  ℹ️  Firmware a plafonné à {lint_confirme}s "
                    "(max réel de ce modèle/firmware)"
                )

            donnees[mac] = entree
            ecrire_capteurs_json(donnees)
            print("  📝 capteurs.json mis à jour.\n")
        else:
            print("  ⚠️  Non vérifiable — capteurs.json non modifié.\n")

    print("✅ Configuration terminée.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrompu par l'utilisateur.")
