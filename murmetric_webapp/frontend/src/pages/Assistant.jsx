import { useState } from "react";
import { api } from "../api.js";
import SelecteurMesure from "../components/SelecteurMesure.jsx";

export default function Assistant() {
  const [selection, setSelection] = useState({ type: "hr_t", mur: "SOCMA 1", couche: "carreau_ext" });
  const [mode, setMode] = useState("explain");
  const [prompt, setPrompt] = useState("");
  const [historique, setHistorique] = useState([]);
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);

  const envoyer = async () => {
    if (!prompt.trim()) return;
    const question = prompt;
    setHistorique((h) => [...h, { role: "utilisateur", texte: question }]);
    setPrompt("");
    setEnCours(true);
    setErreur(null);
    try {
      const resultat = await api.chatAssistant({ mode, prompt: question, selection });
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
          Ancré sur la sélection ci-dessous — jamais sur des points bruts,
          uniquement sur des statistiques pré-agrégées (cf. logique_projet.md
          section 32). Les brouillons de rapport sont à relire avant usage.
        </p>
        <SelecteurMesure valeur={selection} onChange={setSelection} />
        <div className="champ" style={{ marginTop: "0.75rem", maxWidth: "260px" }}>
          <label>Mode</label>
          <select value={mode} onChange={(e) => setMode(e.target.value)}>
            <option value="explain">Explication de la courbe</option>
            <option value="report">Brouillon de rapport d'instrumentation</option>
          </select>
        </div>
      </div>

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
        <div style={{ marginTop: "0.5rem" }}>
          <button onClick={envoyer} disabled={enCours}>{enCours ? "Réflexion..." : "Envoyer"}</button>
        </div>
      </div>
    </div>
  );
}
