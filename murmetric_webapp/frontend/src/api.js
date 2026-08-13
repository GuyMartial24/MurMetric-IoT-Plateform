import { auth } from "./auth.js";

// Vide par défaut = requêtes relatives (même origine que le frontend) —
// c'est le cas en production, où FastAPI sert le build React (cf.
// murmetric_webapp/backend/app/main.py). En dev local, VITE_API_URL pointe
// vers le backend uvicorn sur un port séparé (cf. .env.example).
const BASE_URL = import.meta.env.VITE_API_URL || "";

async function requete(chemin, options = {}) {
  const token = auth.getToken();
  const reponse = await fetch(`${BASE_URL}${chemin}`, {
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    ...options,
  });
  if (reponse.status === 401) {
    auth.deconnecter();
    window.location.href = "/login";
    throw new Error("Session expirée, reconnecte-toi.");
  }
  const corps = await reponse.json().catch(() => null);
  if (!reponse.ok) {
    throw new Error(corps?.detail || `Erreur HTTP ${reponse.status}`);
  }
  return corps;
}

export const api = {
  mesures: (params) => requete(`/api/mesures?${new URLSearchParams(params)}`),
  croisement: (params) => requete(`/api/mesures/croisement?${new URLSearchParams(params)}`),
  croisementLibre: (params) => requete(`/api/mesures/croisement-libre?${new URLSearchParams(params)}`),
  hampel: (params) => requete(`/api/mesures/hampel?${new URLSearchParams(params)}`),
  mesuresValeursTags: (params) => requete(`/api/mesures/valeurs-tags?${new URLSearchParams(params)}`),
  statistiques: (params) => requete(`/api/mesures/statistiques?${new URLSearchParams(params)}`),
  capteursHrT: () => requete("/api/capteurs/hr_t"),
  capteursRetrait: () => requete("/api/capteurs/retrait"),
  modifierCapteurHrT: (mac, champs) =>
    requete(`/api/capteurs/hr_t/${encodeURIComponent(mac)}`, { method: "PUT", body: JSON.stringify(champs) }),
  modifierCapteurRetrait: (canal, champs) =>
    requete(`/api/capteurs/retrait/${encodeURIComponent(canal)}`, { method: "PUT", body: JSON.stringify(champs) }),
  listerTeneurEau: () => requete("/api/teneur_eau"),
  creerTeneurEau: (saisie) => requete("/api/teneur_eau", { method: "POST", body: JSON.stringify(saisie) }),
  corrigerTeneurEau: (correction) => requete("/api/teneur_eau", { method: "PUT", body: JSON.stringify(correction) }),
  chatAssistant: (demande) => requete("/api/assistant/chat", { method: "POST", body: JSON.stringify(demande) }),
  chatAssistantImage: (demande) => requete("/api/assistant/chat-image", { method: "POST", body: JSON.stringify(demande) }),
  login: (username, password) => requete("/api/auth/login", { method: "POST", body: JSON.stringify({ username, password }) }),
  register: (username, password, nomAffiche) =>
    requete("/api/auth/register", { method: "POST", body: JSON.stringify({ username, password, nom_affiche: nomAffiche }) }),
  modifierCompte: (modification) => requete("/api/auth/me", { method: "PUT", body: JSON.stringify(modification) }),
  lireParametres: () => requete("/api/parametres"),
  modifierParametres: (parametres) => requete("/api/parametres", { method: "PUT", body: JSON.stringify(parametres) }),
  monitoringEtat: () => requete("/api/monitoring/etat"),
  monitoringHeartbeats: (pipeline, heures = 24) =>
    requete(`/api/monitoring/heartbeats?${new URLSearchParams({ pipeline, heures })}`),
};
