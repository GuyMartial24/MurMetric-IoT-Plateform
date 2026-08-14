"""Authentification JWT — comptes stockés dans un fichier JSON simple
(échelle de l'outil : quelques utilisateurs internes FRD-CODEM, pas un
SaaS multi-tenant — cf. logique_projet.md section 32). Pas d'auto-
inscription ouverte : le tout premier compte est créé au démarrage via
ADMIN_BOOTSTRAP_USERNAME/PASSWORD (même logique que GF_SECURITY_ADMIN_PASSWORD
pour Grafana), les suivants sont créés par un utilisateur déjà connecté."""

import json
import threading
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import config

_verrou = threading.Lock()
_bearer = HTTPBearer()


def _lire_utilisateurs() -> dict:
    if not config.USERS_JSON.exists():
        return {}
    with open(config.USERS_JSON, encoding="utf-8") as f:
        return json.load(f)


def _ecrire_utilisateurs(utilisateurs: dict) -> None:
    config.USERS_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(config.USERS_JSON, "w", encoding="utf-8") as f:
        json.dump(utilisateurs, f, ensure_ascii=False, indent=2)


def initialiser_bootstrap() -> None:
    """Crée le premier compte admin si users.json est vide et que les
    variables ADMIN_BOOTSTRAP_* sont fournies — no-op sinon (silencieux,
    pas une erreur : permet de démarrer sans auth configurée pendant le dev)."""
    with _verrou:
        utilisateurs = _lire_utilisateurs()
        if (
            utilisateurs
            or not config.ADMIN_BOOTSTRAP_USERNAME
            or not config.ADMIN_BOOTSTRAP_PASSWORD
        ):
            return
        utilisateurs[config.ADMIN_BOOTSTRAP_USERNAME] = {
            "mot_de_passe_hash": bcrypt.hashpw(
                config.ADMIN_BOOTSTRAP_PASSWORD.encode(), bcrypt.gensalt()
            ).decode(),
            "nom_affiche": config.ADMIN_BOOTSTRAP_USERNAME,
            "cree_le": datetime.now(timezone.utc).isoformat(),
        }
        _ecrire_utilisateurs(utilisateurs)


def creer_utilisateur(username: str, password: str, nom_affiche: str) -> None:
    """Crée un compte (appelé par un utilisateur déjà connecté, pas d'auto-inscription)."""
    with _verrou:
        utilisateurs = _lire_utilisateurs()
        if username in utilisateurs:
            raise HTTPException(status_code=409, detail="Ce nom d'utilisateur existe déjà.")
        utilisateurs[username] = {
            "mot_de_passe_hash": bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode(),
            "nom_affiche": nom_affiche or username,
            "cree_le": datetime.now(timezone.utc).isoformat(),
        }
        _ecrire_utilisateurs(utilisateurs)


def modifier_compte(
    username_actuel: str,
    mot_de_passe_actuel: str,
    nouveau_username: str | None,
    nouveau_password: str | None,
    nouveau_nom_affiche: str | None,
) -> str:
    """Modifie le compte de l'utilisateur connecté (jamais celui d'un autre —
    `username_actuel` vient toujours du jeton JWT, pas d'un champ de
    formulaire). Retourne le username final (inchangé ou renommé)."""
    with _verrou:
        utilisateurs = _lire_utilisateurs()
        compte = utilisateurs.get(username_actuel)
        if not compte or not bcrypt.checkpw(
            mot_de_passe_actuel.encode(), compte["mot_de_passe_hash"].encode()
        ):
            raise HTTPException(status_code=401, detail="Mot de passe actuel incorrect.")

        if nouveau_password:
            compte["mot_de_passe_hash"] = bcrypt.hashpw(
                nouveau_password.encode(), bcrypt.gensalt()
            ).decode()
        if nouveau_nom_affiche:
            compte["nom_affiche"] = nouveau_nom_affiche

        username_final = username_actuel
        if nouveau_username and nouveau_username != username_actuel:
            if nouveau_username in utilisateurs:
                raise HTTPException(status_code=409, detail="Ce nom d'utilisateur existe déjà.")
            del utilisateurs[username_actuel]
            username_final = nouveau_username

        utilisateurs[username_final] = compte
        _ecrire_utilisateurs(utilisateurs)
        return username_final


def verifier_identifiants(username: str, password: str) -> dict | None:
    """Vérifie un couple username/mot de passe, retourne le compte si valide sinon None."""
    utilisateurs = _lire_utilisateurs()
    compte = utilisateurs.get(username)
    if not compte or not bcrypt.checkpw(password.encode(), compte["mot_de_passe_hash"].encode()):
        return None
    return compte


def creer_jeton(username: str) -> str:
    """Génère un jeton JWT signé, valide JWT_EXPIRATION_HEURES heures."""
    expiration = datetime.now(timezone.utc) + timedelta(hours=config.JWT_EXPIRATION_HEURES)
    return jwt.encode(
        {"sub": username, "exp": expiration}, config.JWT_SECRET_KEY, algorithm=config.JWT_ALGORITHM
    )


def utilisateur_courant(identifiants: HTTPAuthorizationCredentials = Depends(_bearer)) -> dict:
    """Dépendance FastAPI — protège une route (`Depends(utilisateur_courant)`),
    retourne {"username": ..., "nom_affiche": ...} si le jeton est valide."""
    try:
        payload = jwt.decode(
            identifiants.credentials, config.JWT_SECRET_KEY, algorithms=[config.JWT_ALGORITHM]
        )
    except jwt.PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Jeton invalide ou expiré.") from exc

    username = payload.get("sub")
    utilisateurs = _lire_utilisateurs()
    compte = utilisateurs.get(username)
    if not compte:
        raise HTTPException(status_code=401, detail="Utilisateur inconnu.")
    return {"username": username, "nom_affiche": compte["nom_affiche"]}
