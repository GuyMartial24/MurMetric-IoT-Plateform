"""Point d'entrée FastAPI — squelette de l'interface applicative unifiée
(section 32 de logique_projet.md)."""
import shutil
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from . import config
from .auth import initialiser_bootstrap
from .routers import assistant, auth, capteurs, mesures, parametres, teneur_eau


def _amorcer_capteurs() -> None:
    """Copier capteurs.json/capteurs_retrait.json de l'image vers le volume
    persistant au tout premier démarrage (volume vide juste après création
    du PVC) — ensuite le volume est seul relu, jamais réécrasé par l'image
    même si sa copie "amorce" change à un déploiement ultérieur (cf.
    config.py, chantier "source unique" du 13/08/2026)."""
    for cible, source in (
        (config.CAPTEURS_JSON, config.CAPTEURS_JSON_SEED),
        (config.CAPTEURS_RETRAIT_JSON, config.CAPTEURS_RETRAIT_JSON_SEED),
    ):
        if not cible.exists() and source.exists():
            cible.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(source, cible)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    initialiser_bootstrap()
    _amorcer_capteurs()
    yield


app = FastAPI(title="MurMetric API", version="0.1.0", lifespan=lifespan)

# Dev uniquement (serveur Vite React sur un port différent) — à restreindre
# une fois l'auth (section 32, "Non tranché à ce stade") et le déploiement
# k3s définis.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(mesures.router)
app.include_router(teneur_eau.router)
app.include_router(capteurs.router)
app.include_router(assistant.router)
app.include_router(auth.router)
app.include_router(parametres.router)


@app.get("/api/health")
def health() -> dict:
    return {"statut": "ok"}


# Sert le build React (dist/) copié à côté de ce fichier par le Dockerfile —
# absent en dev local (frontend servi séparément par `npm run dev`, cf.
# CORS ci-dessus), donc monté seulement si présent.
_DIST_DIR = Path(__file__).resolve().parent / "static"
if _DIST_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=_DIST_DIR / "assets"), name="assets")

    @app.get("/{chemin_complet:path}")
    def servir_frontend(chemin_complet: str):
        # Fichiers statiques à la racine du build (favicon.svg, etc.) —
        # sans ce cas, la route générique interceptait tout et renvoyait
        # index.html même pour /favicon.svg (bug trouvé le 12/08/2026).
        # resolve() + vérification du parent : évite qu'un chemin_complet
        # contenant ".." serve un fichier hors de dist/.
        chemin_fichier = (_DIST_DIR / chemin_complet).resolve()
        if chemin_complet and chemin_fichier.is_file() and chemin_fichier.is_relative_to(_DIST_DIR):
            return FileResponse(chemin_fichier)
        return FileResponse(_DIST_DIR / "index.html")
