import { useState } from "react";
import { api } from "../api.js";
import GraphiqueSVG from "../components/GraphiqueSVG.jsx";
import Nomogramme from "../components/Nomogramme.jsx";
import Nomogramme3D from "../components/Nomogramme3D.jsx";
import SelecteurMesure from "../components/SelecteurMesure.jsx";

const CHAMP_PRINCIPAL = { hr_t: "temperature", retrait: "valeur_filtree", teneur_eau: "teneur_eau_pourcent" };

export default function VueEnsemble() {
  const [selection, setSelection] = useState({ type: "hr_t", mur: "SOCMA 1" });
  const [points, setPoints] = useState([]);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);
  const [mode3D, setMode3D] = useState(false);

  const charger = async () => {
    setEnCours(true);
    setErreur(null);
    try {
      const params = Object.fromEntries(Object.entries(selection).filter(([, v]) => v));
      const resultat = await api.mesures(params);
      setPoints(resultat.points);
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <div>
      <div className="carte">
        <h2>Vue d'ensemble</h2>
        <SelecteurMesure valeur={selection} onChange={setSelection} />
        <button onClick={charger} disabled={enCours} style={{ marginTop: "0.75rem" }}>
          {enCours ? "Chargement..." : "Charger la courbe temporelle"}
        </button>
        {erreur && <p className="erreur">{erreur}</p>}
      </div>
      {points.length > 0 && (
        <div className="carte">
          <h3 style={{ marginTop: 0 }}>Courbe valeur/temps</h3>
          <GraphiqueSVG points={points} champ={CHAMP_PRINCIPAL[selection.type]} />
        </div>
      )}
      {(selection.type === "hr_t" || selection.type === "retrait") && (
        <div className="carte">
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <h3 style={{ margin: 0 }}>Nomogramme — grandeurs croisées</h3>
            <div>
              <button onClick={() => setMode3D(false)} disabled={!mode3D} style={{ marginRight: "0.5rem" }}>2D</button>
              <button onClick={() => setMode3D(true)} disabled={mode3D}>3D (HR/T + retrait)</button>
            </div>
          </div>
          <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
            {mode3D
              ? "Compose librement 3 grandeurs (HR/T, retrait, temps) — glisse pour tourner, molette pour zoomer, survole un point pour lire ses valeurs."
              : "Compose librement 2 grandeurs (HR/T, retrait, temps) — survole un point pour lire sa valeur par projection sur les axes."}
          </p>
          {mode3D ? (
            <Nomogramme3D mur={selection.mur} couche={selection.couche} />
          ) : (
            <Nomogramme mur={selection.mur} couche={selection.couche} />
          )}
        </div>
      )}
    </div>
  );
}
