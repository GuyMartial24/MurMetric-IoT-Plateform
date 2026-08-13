import { useRef, useState } from "react";
import { api } from "../api.js";
import GraphiqueSVG from "../components/GraphiqueSVG.jsx";
import SelecteurMesure from "../components/SelecteurMesure.jsx";
import { svgVersDataUrl } from "../exportGraphique.js";

const CHAMP_PRINCIPAL = { hr_t: "temperature", retrait: "valeur_filtree", teneur_eau: "teneur_eau_pourcent" };

export default function Assistant() {
  const [selection, setSelection] = useState({ type: "hr_t", mur: "SOCMA 1", couche: "carreau_ext" });
  const [points, setPoints] = useState([]);
  const [mode, setMode] = useState("explain");
  const [prompt, setPrompt] = useState("");
  const [historique, setHistorique] = useState([]);
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);
  const graphiqueRef = useRef(null);

  const chargerCourbe = async () => {
    setErreur(null);
    try {
      const params = Object.fromEntries(Object.entries(selection).filter(([, v]) => v));
      const resultat = await api.mesures(params);
      setPoints(resultat.points);
    } catch (e) {
      setErreur(e.message);
    }
  };

  const envoyer = async (avecGraphique) => {
    if (!prompt.trim()) return;
    const question = prompt;
    setHistorique((h) => [...h, { role: "utilisateur", texte: avecGraphique ? `📎 ${question}` : question }]);
    setPrompt("");
    setEnCours(true);
    setErreur(null);
    try {
      let resultat;
      if (avecGraphique) {
        const imageDataUri = await svgVersDataUrl(graphiqueRef.current);
        resultat = await api.chatAssistantImage({ mode, prompt: question, image_data_uri: imageDataUri, selection });
      } else {
        resultat = await api.chatAssistant({ mode, prompt: question, selection });
      }
      setHistorique((h) => [...h, { role: "assistant", texte: resultat.reponse }]);
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <div>
      <div className="carte">
        <h2>Assistant IA</h2>
        <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
          Ancré sur la sélection ci-dessous — jamais sur des points bruts, uniquement sur des statistiques
          pré-agrégées (ou, si tu joins le graphique, sur l'image déjà tracée par l'appli). Les brouillons de
          rapport sont à relire avant usage.
        </p>
        <SelecteurMesure valeur={selection} onChange={setSelection} />
        <div className="champ" style={{ marginTop: "0.75rem", maxWidth: "260px" }}>
          <label>Mode</label>
          <select value={mode} onChange={(e) => setMode(e.target.value)}>
            <option value="explain">Explication de la courbe</option>
            <option value="report">Brouillon de rapport d'instrumentation</option>
          </select>
        </div>
        <button onClick={chargerCourbe} style={{ marginTop: "0.75rem" }}>
          Charger la courbe (pour l'analyse visuelle, optionnel)
        </button>
      </div>

      {points.length > 0 && (
        <div className="carte">
          <h3 style={{ marginTop: 0 }}>Courbe — {selection.type}</h3>
          <GraphiqueSVG ref={graphiqueRef} points={points} champ={CHAMP_PRINCIPAL[selection.type]} />
        </div>
      )}

      <div className="carte">
        {historique.map((m, i) => (
          <div key={i} className={`chat-message ${m.role}`}>{m.texte}</div>
        ))}
        {erreur && <p className="erreur">{erreur}</p>}
        <textarea
          rows={3}
          style={{ width: "100%" }}
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          placeholder="ex. Explique l'évolution de la température sur cette sélection"
        />
        <div style={{ marginTop: "0.5rem", display: "flex", gap: "0.5rem" }}>
          <button onClick={() => envoyer(false)} disabled={enCours}>{enCours ? "Réflexion..." : "Envoyer"}</button>
          <button
            onClick={() => envoyer(true)}
            disabled={enCours || points.length === 0}
            title={points.length === 0 ? "Charge d'abord la courbe ci-dessus" : "Envoie l'image du graphique à l'IA (analyse visuelle)"}
          >
            {enCours ? "Réflexion..." : "Envoyer avec le graphique"}
          </button>
        </div>
      </div>
    </div>
  );
}
