import { useState } from "react";
import { api } from "../api.js";
import GraphiqueSVG from "../components/GraphiqueSVG.jsx";
import Nomogramme from "../components/Nomogramme.jsx";
import SelecteurMesure from "../components/SelecteurMesure.jsx";

const CHAMP_PRINCIPAL = { hr_t: "temperature", retrait: "valeur_filtree", teneur_eau: "teneur_eau_pourcent" };

export default function VueEnsemble() {
  const [selection, setSelection] = useState({ type: "hr_t", mur: "SOCMA 1" });
  const [points, setPoints] = useState([]);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);

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
          <h3 style={{ marginTop: 0 }}>Nomogramme — grandeurs croisées</h3>
          <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
            Croise deux grandeurs l'une contre l'autre (pas contre le temps) — survole un point pour lire sa valeur par projection sur les axes.
          </p>
          <Nomogramme type={selection.type} mur={selection.mur} couche={selection.couche} position={selection.position} />
        </div>
      )}
    </div>
  );
}
