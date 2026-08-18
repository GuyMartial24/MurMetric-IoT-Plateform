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

// Téléchargement direct d'un CSV généré côté serveur (routes /api/export/*) —
// distinct de requete() : la réponse est déjà un fichier CSV, pas du JSON.
async function telechargerExport(chemin) {
  const token = auth.getToken();
  const reponse = await fetch(`${BASE_URL}${chemin}`, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  if (reponse.status === 401) {
    auth.deconnecter();
    window.location.href = "/login";
    throw new Error("Session expirée, reconnecte-toi.");
  }
  if (!reponse.ok) {
    const corps = await reponse.json().catch(() => null);
    throw new Error(corps?.detail || `Erreur HTTP ${reponse.status}`);
  }
  const entete = reponse.headers.get("Content-Disposition") || "";
  const nomFichier = /filename="([^"]+)"/.exec(entete)?.[1] || "export.csv";
  const blob = await reponse.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = nomFichier;
  a.click();
  URL.revokeObjectURL(url);
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
  chatAssistantImage: (demande) =>
    requete("/api/assistant/chat-image", { method: "POST", body: JSON.stringify(demande) }),
  login: (username, password) =>
    requete("/api/auth/login", { method: "POST", body: JSON.stringify({ username, password }) }),
  register: (username, password, nomAffiche) =>
    requete("/api/auth/register", {
      method: "POST",
      body: JSON.stringify({ username, password, nom_affiche: nomAffiche }),
    }),
  modifierCompte: (modification) => requete("/api/auth/me", { method: "PUT", body: JSON.stringify(modification) }),
  lireParametres: () => requete("/api/parametres"),
  modifierParametres: (parametres) => requete("/api/parametres", { method: "PUT", body: JSON.stringify(parametres) }),
  monitoringEtat: () => requete("/api/monitoring/etat"),
  monitoringHeartbeats: (pipeline, heures = 24) =>
    requete(`/api/monitoring/heartbeats?${new URLSearchParams({ pipeline, heures })}`),
  monitoringEspaceDisque: (jours = 30) => requete(`/api/monitoring/espace-disque?${new URLSearchParams({ jours })}`),
  exporterRetrait: (params) => telechargerExport(`/api/export/retrait?${new URLSearchParams(params)}`),
  exporterHrT: (params) => telechargerExport(`/api/export/hr_t?${new URLSearchParams(params)}`),
  exporterTeneurEau: (params) => telechargerExport(`/api/export/teneur_eau?${new URLSearchParams(params)}`),
  demarrerTacheRetrait: (params) =>
    requete(`/api/export/retrait/tache?${new URLSearchParams(params)}`, { method: "POST" }),
  etatTacheRetrait: (tacheId) => requete(`/api/export/retrait/tache/${tacheId}`),
  telechargerTacheRetrait: (tacheId) => telechargerExport(`/api/export/retrait/tache/${tacheId}/telecharger`),
};
