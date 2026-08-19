import { createContext, useContext, useState } from "react";

// État préservé de certaines pages entre deux navigations (changement
// d'onglet), sans rien persister au rechargement de la page ni entre
// onglets/appareils différents — porté ici, au-dessus des <Routes> dans
// App.jsx, précisément pour survivre au démontage/remontage des pages
// elles-mêmes à chaque navigation (comportement normal de React Router,
// inchangé). Ciblé sur Assistant IA et Vue d'ensemble (demande explicite) —
// pas toutes les pages, pour ne pas faire tourner en permanence celles qui
// interrogent régulièrement le serveur.
const EtatPagesContext = createContext(null);

export function FournisseurEtatPages({ children }) {
  const [assistantSelection, setAssistantSelection] = useState({
    type: "hr_t",
    champ: "temperature",
    mur: "SOCMA 1",
    couche: "",
  });
  const [assistantPoints, setAssistantPoints] = useState(null);
  const [assistantMode, setAssistantMode] = useState("explain");
  const [assistantPrompt, setAssistantPrompt] = useState("");
  const [assistantHistorique, setAssistantHistorique] = useState([]);
  const [assistantImageJointe, setAssistantImageJointe] = useState(null);
  const [assistantDernierEchec, setAssistantDernierEchec] = useState(null);

  const [vueSelection, setVueSelection] = useState({ type: "hr_t", champ: "temperature", mur: "SOCMA 1" });
  const [vuePoints, setVuePoints] = useState(null);
  const [vueMode3D, setVueMode3D] = useState(false);

  const valeur = {
    assistant: {
      selection: assistantSelection,
      setSelection: setAssistantSelection,
      points: assistantPoints,
      setPoints: setAssistantPoints,
      mode: assistantMode,
      setMode: setAssistantMode,
      prompt: assistantPrompt,
      setPrompt: setAssistantPrompt,
      historique: assistantHistorique,
      setHistorique: setAssistantHistorique,
      imageJointe: assistantImageJointe,
      setImageJointe: setAssistantImageJointe,
      dernierEchec: assistantDernierEchec,
      setDernierEchec: setAssistantDernierEchec,
    },
    vueEnsemble: {
      selection: vueSelection,
      setSelection: setVueSelection,
      points: vuePoints,
      setPoints: setVuePoints,
      mode3D: vueMode3D,
      setMode3D: setVueMode3D,
    },
  };

  return <EtatPagesContext.Provider value={valeur}>{children}</EtatPagesContext.Provider>;
}

export function useEtatAssistant() {
  return useContext(EtatPagesContext).assistant;
}

export function useEtatVueEnsemble() {
  return useContext(EtatPagesContext).vueEnsemble;
}
