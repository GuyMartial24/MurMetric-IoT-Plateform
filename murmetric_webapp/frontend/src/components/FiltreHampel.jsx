import { useState } from "react";
import { api } from "../api.js";
import { CANAUX_RETRAIT } from "../nomogrammeAxes.js";

// Filtre de Hampel recalculé à la volée sur le retrait BRUT — répond à une
// question explicite de l'utilisateur (13/08/2026) : le seuil d'ingestion
// (HAMPEL_SEUIL_K, fixe côté ingestion_dewesoft_dxd.py) n'est pas ajustable
// depuis l'interface. Ici on ne touche à rien en base — juste un recalcul
// pour comparer visuellement différents réglages sur une courte période
// (max 2h côté backend : mesures_dewesoft est à 100 Hz, un filtre point par
// point ne peut pas s'appuyer sur une agrégation comme les autres vues).
function heureLocaleDatetimeInput(date) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

const MAINTENANT = new Date();
const IL_Y_A_1H = new Date(MAINTENANT.getTime() - 60 * 60 * 1000);

export default function FiltreHampel({ mur }) {
  const [canal, setCanal] = useState("HA1");
  const [debut, setDebut] = useState(heureLocaleDatetimeInput(IL_Y_A_1H));
  const [fin, setFin] = useState(heureLocaleDatetimeInput(MAINTENANT));
  const [fenetre, setFenetre] = useState(10);
  const [seuilK, setSeuilK] = useState(8);
  const [resultat, setResultat] = useState(null);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const appliquer = async () => {
    setEnCours(true);
    setErreur(null);
    try {
      const r = await api.hampel({
        mur,
        canal_nom: canal,
        debut: new Date(debut).toISOString(),
        fin: new Date(fin).toISOString(),
        fenetre,
        seuil_k: seuilK,
      });
      setResultat(r);
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  const points = resultat?.points ?? [];

  return (
    <div>
      <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
        Recalcule le filtre de Hampel à la volée sur les valeurs brutes (rien n'est modifié en base) — pour comparer
        différents réglages de fenêtre/seuil sans toucher au réglage d'ingestion. Période limitée à 2h (données à 100 Hz).
      </p>
      <div className="selection-form" style={{ marginBottom: "0.75rem" }}>
        <div className="champ">
          <label>Canal</label>
          <select value={canal} onChange={(e) => setCanal(e.target.value)}>
            {CANAUX_RETRAIT.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
        <div className="champ">
          <label>Début</label>
          <input type="datetime-local" value={debut} onChange={(e) => setDebut(e.target.value)} />
        </div>
        <div className="champ">
          <label>Fin</label>
          <input type="datetime-local" value={fin} onChange={(e) => setFin(e.target.value)} />
        </div>
        <div className="champ">
          <label>Fenêtre (échantillons)</label>
          <input type="number" min="1" max="200" value={fenetre} onChange={(e) => setFenetre(Number(e.target.value))} />
        </div>
        <div className="champ">
          <label>Seuil K</label>
          <input type="number" min="0.1" step="0.1" value={seuilK} onChange={(e) => setSeuilK(Number(e.target.value))} />
        </div>
      </div>
      <button onClick={appliquer} disabled={enCours}>{enCours ? "Calcul..." : "Appliquer"}</button>
      {erreur && <p className="erreur">{erreur}</p>}
      {resultat && (
        <>
          <p style={{ marginTop: "0.75rem" }}>
            {resultat.nb_points} points · <span style={{ color: "#ff8080" }}>{resultat.nb_aberrants} détecté(s) comme aberrant(s)</span> avec fenêtre={resultat.fenetre}, K={resultat.seuil_k}
          </p>
          {points.length > 0 && <GraphiqueHampel points={points} />}
        </>
      )}
    </div>
  );
}

function GraphiqueHampel({ points }) {
  const largeur = 900, hauteur = 320, marge = 40;
  const temps = points.map((p) => new Date(p.time).getTime());
  const brut = points.map((p) => p.brut);
  const filtre = points.map((p) => p.filtre_ajuste);
  const tMin = Math.min(...temps), tMax = Math.max(...temps);
  const vMin = Math.min(...brut, ...filtre), vMax = Math.max(...brut, ...filtre);

  const x = (t) => marge + ((t - tMin) / (tMax - tMin || 1)) * (largeur - 2 * marge);
  const y = (v) => hauteur - marge - ((v - vMin) / (vMax - vMin || 1)) * (hauteur - 2 * marge);

  const cheminBrut = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(temps[i])},${y(p.brut)}`).join(" ");
  const cheminFiltre = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(temps[i])},${y(p.filtre_ajuste)}`).join(" ");

  return (
    <svg viewBox={`0 0 ${largeur} ${hauteur}`} width="100%" height={hauteur}>
      <path d={cheminBrut} fill="none" stroke="#5a6270" strokeWidth="1" />
      <path d={cheminFiltre} fill="none" stroke="#7fd4ff" strokeWidth="1.5" />
      {points.map((p, i) => p.aberrant && (
        <circle key={i} cx={x(temps[i])} cy={y(p.brut)} r="3" fill="#ff8080" />
      ))}
      <text x={marge} y={16} fill="#5a6270" fontSize="12">— brut</text>
      <text x={marge + 60} y={16} fill="#7fd4ff" fontSize="12">— filtré (ajusté)</text>
      <text x={marge + 190} y={16} fill="#ff8080" fontSize="12">● point corrigé</text>
    </svg>
  );
}
