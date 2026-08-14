"""Routes d'authentification — connexion, création de compte (réservée aux
utilisateurs déjà connectés) et gestion du compte courant."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..auth import (
    creer_jeton,
    creer_utilisateur,
    modifier_compte,
    utilisateur_courant,
    verifier_identifiants,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])


class Identifiants(BaseModel):
    """Couple username/mot de passe soumis à la connexion."""

    username: str
    password: str


class NouvelUtilisateur(BaseModel):
    """Données de création d'un compte."""

    username: str
    password: str
    nom_affiche: str = ""


class ModificationCompte(BaseModel):
    """Modification du compte de l'utilisateur connecté (mot de passe actuel requis)."""

    mot_de_passe_actuel: str
    nouveau_username: str | None = None
    nouveau_password: str | None = None
    nouveau_nom_affiche: str | None = None


@router.post("/login")
def login(identifiants: Identifiants) -> dict:
    """Vérifie les identifiants et retourne un jeton JWT si valides."""
    compte = verifier_identifiants(identifiants.username, identifiants.password)
    if not compte:
        raise HTTPException(status_code=401, detail="Identifiants incorrects.")
    return {
        "access_token": creer_jeton(identifiants.username),
        "token_type": "bearer",
        "nom_affiche": compte["nom_affiche"],
    }


@router.post("/register")
def register(nouveau: NouvelUtilisateur, _actuel: dict = Depends(utilisateur_courant)) -> dict:
    """Pas d'auto-inscription ouverte — seul un utilisateur déjà connecté
    peut créer un nouveau compte (cf. app/auth.py)."""
    creer_utilisateur(nouveau.username, nouveau.password, nouveau.nom_affiche)
    return {"statut": "ok"}


@router.get("/me")
def me(actuel: dict = Depends(utilisateur_courant)) -> dict:
    """Retourne l'identité de l'utilisateur associé au jeton envoyé."""
    return actuel


@router.put("/me")
def modifier_mon_compte(
    modification: ModificationCompte, actuel: dict = Depends(utilisateur_courant)
) -> dict:
    """Change son propre mot de passe/nom d'utilisateur/nom affiché — exige
    le mot de passe actuel (cf. app/auth.py). Un nouveau jeton est renvoyé
    si le username a changé (l'ancien jeton référence l'ancien username)."""
    username_final = modifier_compte(
        actuel["username"],
        modification.mot_de_passe_actuel,
        modification.nouveau_username,
        modification.nouveau_password,
        modification.nouveau_nom_affiche,
    )
    return {"statut": "ok", "access_token": creer_jeton(username_final)}
