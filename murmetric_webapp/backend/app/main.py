"""Point d'entrée FastAPI — squelette de l'interface applicative unifiée
(section 32 de logique_projet.md)."""
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .auth import initialiser_bootstrap
from .routers import assistant, auth, capteurs, mesures, parametres, teneur_eau


@asynccontextmanager
async def lifespan(_app: FastAPI):
    initialiser_bootstrap()
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
        return FileResponse(_DIST_DIR / "index.html")
