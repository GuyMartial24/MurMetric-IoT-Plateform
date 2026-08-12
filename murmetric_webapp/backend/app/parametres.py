"""Paramètres modifiables depuis l'interface (ex. clé API Groq) — stockés à
côté de users.json sur le même volume persistant, pas seulement en variable
d'environnement figée au déploiement. Les valeurs d'environnement
(GROQ_API_KEY/GROQ_MODEL) ne servent plus que de valeur de secours initiale
si rien n'a encore été défini depuis l'interface."""
import json
import threading

from . import config

_verrou = threading.Lock()
_FICHIER = config.USERS_JSON.parent / "parametres.json"


def _lire() -> dict:
    if not _FICHIER.exists():
        return {}
    with open(_FICHIER, encoding="utf-8") as f:
        return json.load(f)


def _ecrire(parametres: dict) -> None:
    _FICHIER.parent.mkdir(parents=True, exist_ok=True)
    with open(_FICHIER, "w", encoding="utf-8") as f:
        json.dump(parametres, f, ensure_ascii=False, indent=2)


def obtenir_cle_groq() -> str:
    return _lire().get("groq_api_key") or config.GROQ_API_KEY


def obtenir_modele_groq() -> str:
    return _lire().get("groq_model") or config.GROQ_MODEL


def definir_parametres_groq(cle_api: str | None, modele: str | None) -> None:
    with _verrou:
        parametres = _lire()
        if cle_api:
            parametres["groq_api_key"] = cle_api
        if modele:
            parametres["groq_model"] = modele
        _ecrire(parametres)


def masquer(valeur: str) -> str:
    if not valeur:
        return ""
    return f"...{valeur[-4:]}" if len(valeur) > 4 else "****"
