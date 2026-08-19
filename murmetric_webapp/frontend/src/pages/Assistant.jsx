import { useRef, useState } from "react";
import { api } from "../api.js";
import GraphiqueSVG from "../components/GraphiqueSVG.jsx";
import SelecteurMesure from "../components/SelecteurMesure.jsx";
import { useEtatAssistant } from "../EtatPagesContext.jsx";
import { svgVersDataUrl } from "../exportGraphique.js";
import { libelleGrandeur } from "../nomogrammeAxes.js";

// Au-delà, la requête risque d'échouer côté API Gemini (charge utile trop
// lourde une fois encodée en base64) — mieux vaut prévenir côté client
// qu'échouer après l'envoi.
const TAILLE_MAX_IMAGE = 8 * 1024 * 1024; // 8 Mo

function fichierVersDataUrl(fichier) {
  return new Promise((resolve, reject) => {
    const lecteur = new FileReader();
    lecteur.onload = () => resolve(lecteur.result);
    lecteur.onerror = () => reject(new Error("Lecture de l'image échouée."));
    lecteur.readAsDataURL(fichier);
  });
}

// Le prompt système demande à l'assistant de toujours isoler son rappel
// "lecture assistée par IA" / "brouillon à valider" dans son propre
// paragraphe commençant par "Note : " — affiché ici en italique/discret
// pour le distinguer visuellement du reste de la réponse.
function TexteAssistant({ texte }) {
  const paragraphes = texte.split(/\n{2,}/);
  return paragraphes.map((paragraphe, i) => {
    const estNote = /^note\s*:/i.test(paragraphe.trim());
    return (
      <p key={i} style={estNote ? { fontStyle: "italic", color: "#a0a6b5", fontSize: "0.85rem" } : undefined}>
        {paragraphe}
      </p>
    );
  });
}

export default function Assistant() {
  // Sélection/courbe/conversation/etc. préservées en changeant d'onglet
  // (EtatPagesContext) — enCours/enCoursCourbe/erreur restent locaux,
  // purement transitoires (n'ont pas de sens une fois la page démontée).
  const {
    selection,
    setSelection,
    points,
    setPoints,
    mode,
    setMode,
    prompt,
    setPrompt,
    historique,
    setHistorique,
    imageJointe,
    setImageJointe,
    dernierEchec,
    setDernierEchec,
  } = useEtatAssistant();
  const [enCours, setEnCours] = useState(false);
  const [enCoursCourbe, setEnCoursCourbe] = useState(false);
  const [erreur, setErreur] = useState(null);
  const graphiqueRef = useRef(null);
  const inputFichierRef = useRef(null);

  const attacherImage = async (fichier) => {
    if (!fichier || !fichier.type.startsWith("image/")) return;
    if (fichier.size > TAILLE_MAX_IMAGE) {
      setErreur("Image trop volumineuse (max 8 Mo) — réduis sa résolution avant de la joindre.");
      return;
    }
    try {
      setImageJointe(await fichierVersDataUrl(fichier));
      setErreur(null);
    } catch (e) {
      setErreur(e.message);
    }
  };

  const collerImage = (e) => {
    const item = Array.from(e.clipboardData.items).find((it) => it.type.startsWith("image/"));
    if (!item) return; // pas d'image dans le presse-papiers : laisse le collage de texte normal se produire
    e.preventDefault();
    attacherImage(item.getAsFile());
  };

  const chargerCourbe = async () => {
    setEnCoursCourbe(true);
    setErreur(null);
    try {
      const params = Object.fromEntries(Object.entries(selection).filter(([, v]) => v));
      const resultat = await api.mesures(params);
      setPoints(resultat.points);
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCoursCourbe(false);
    }
  };

  // Exécute réellement l'envoi — factorisé pour pouvoir être rejoué à
  // l'identique par "Réessayer" sans redemander l'image/le texte à
  // l'utilisateur (utile pour une erreur transitoire, ex. Gemini
  // temporairement surchargé, cf. logique_projet.md section 36).
  const executerEnvoi = async (question, modeEnvoi, source, imageDataUri) => {
    setEnCours(true);
    setErreur(null);
    try {
      const resultat =
        source === "texte"
          ? await api.chatAssistant({ mode: modeEnvoi, prompt: question, selection })
          : await api.chatAssistantImage({
              mode: modeEnvoi,
              prompt: question,
              image_data_uri: imageDataUri,
              // Le graphique auto-capturé correspond à la sélection courante ;
              // une image collée/importée est étrangère à l'appli — l'associer
              // à des statistiques sans rapport serait trompeur pour l'IA.
              ...(source === "graphique" ? { selection } : {}),
            });
      setHistorique((h) => [...h, { role: "assistant", texte: resultat.reponse }]);
      setDernierEchec(null);
    } catch (e) {
      setErreur(e.message);
      setDernierEchec({ question, mode: modeEnvoi, source, imageDataUri });
    } finally {
      setEnCours(false);
    }
  };

  const envoyer = async (source) => {
    // source: "texte" | "graphique" (courbe de l'appli, auto-capturée) | "image" (collée/importée)
    if (!prompt.trim()) return;
    const question = prompt;
    const etiquette = source === "graphique" ? "graphique joint" : source === "image" ? "image jointe" : null;
    setHistorique((h) => [...h, { role: "utilisateur", texte: question, etiquette }]);
    setPrompt("");
    let imageDataUri = null;
    if (source === "graphique") {
      imageDataUri = await svgVersDataUrl(graphiqueRef.current);
    } else if (source === "image") {
      imageDataUri = imageJointe;
      setImageJointe(null);
    }
    await executerEnvoi(question, mode, source, imageDataUri);
  };

  const reessayer = () => {
    if (!dernierEchec) return;
    const { question, mode: modeEnvoi, source, imageDataUri } = dernierEchec;
    executerEnvoi(question, modeEnvoi, source, imageDataUri);
  };

  return (
    <div>
      <div className="carte">
        <h2>Assistant IA</h2>
        <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
          Ancré sur la sélection ci-dessous — jamais sur des points bruts, uniquement sur des statistiques pré-agrégées,
          sauf si tu joins une image (le graphique de l'appli, ou une capture collée/importée) : l'IA l'analyse alors
          directement. Les brouillons de rapport sont à relire avant usage.
        </p>
        <SelecteurMesure valeur={selection} onChange={setSelection} />
        <div className="champ" style={{ marginTop: "0.75rem", maxWidth: "260px" }}>
          <label>Mode</label>
          <select value={mode} onChange={(e) => setMode(e.target.value)}>
            <option value="explain">Explication de la courbe</option>
            <option value="report">Brouillon de rapport d'instrumentation</option>
          </select>
        </div>
        <button onClick={chargerCourbe} disabled={enCoursCourbe} style={{ marginTop: "0.75rem" }}>
          {enCoursCourbe ? "Chargement..." : "Charger la courbe (pour l'analyse visuelle, optionnel)"}
        </button>
      </div>

      {points && points.length > 0 && (
        <div className="carte">
          <h3 style={{ marginTop: 0 }}>Courbe — {libelleGrandeur(`${selection.type}:${selection.champ}`)}</h3>
          <GraphiqueSVG ref={graphiqueRef} points={points} champ={selection.champ} />
        </div>
      )}
      {points && points.length === 0 && (
        <div className="carte">
          <p>
            Aucune donnée pour cette sélection sur la période choisie — essaie d'élargir la période, ou vérifie le
            mur/la couche (la dernière mesure disponible peut être plus ancienne que la période demandée).
          </p>
        </div>
      )}

      <div className="carte">
        {historique.map((m, i) => (
          <div key={i} className={`chat-message ${m.role}`}>
            {m.etiquette && (
              <span
                style={{
                  display: "inline-block",
                  fontSize: "0.72rem",
                  color: "#a0a6b5",
                  border: "1px solid #3a4152",
                  borderRadius: "4px",
                  padding: "0 0.35rem",
                  marginRight: "0.4rem",
                }}
              >
                {m.etiquette}
              </span>
            )}
            {m.role === "assistant" ? <TexteAssistant texte={m.texte} /> : m.texte}
          </div>
        ))}
        {erreur && (
          <p className="erreur">
            {erreur}
            {dernierEchec && (
              <button onClick={reessayer} disabled={enCours} style={{ marginLeft: "0.6rem" }}>
                Réessayer
              </button>
            )}
          </p>
        )}
        {imageJointe && (
          <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "0.5rem" }}>
            <img
              src={imageJointe}
              alt="Image jointe"
              style={{ maxHeight: "70px", borderRadius: "4px", border: "1px solid #3a4152" }}
            />
            <span style={{ fontSize: "0.8rem", color: "#a0a6b5" }}>
              Image jointe — sera envoyée avec le prochain message.
            </span>
            <button onClick={() => setImageJointe(null)} style={{ fontSize: "0.8rem" }}>
              Retirer
            </button>
          </div>
        )}
        <input
          type="file"
          accept="image/*"
          ref={inputFichierRef}
          style={{ display: "none" }}
          onChange={(e) => {
            attacherImage(e.target.files[0]);
            e.target.value = "";
          }}
        />
        <textarea
          rows={3}
          style={{ width: "100%" }}
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          onPaste={collerImage}
          placeholder="ex. Explique l'évolution de la température sur cette sélection (Ctrl+V pour coller une capture)"
        />
        <div style={{ marginTop: "0.5rem", display: "flex", gap: "0.5rem" }}>
          <button onClick={() => envoyer(imageJointe ? "image" : "texte")} disabled={enCours}>
            {enCours ? "Réflexion..." : "Envoyer"}
          </button>
          <button
            onClick={() => envoyer("graphique")}
            disabled={enCours || !points || points.length === 0}
            title={
              !points || points.length === 0
                ? "Charge d'abord la courbe ci-dessus"
                : "Envoie l'image du graphique à l'IA (analyse visuelle)"
            }
          >
            {enCours ? "Réflexion..." : "Envoyer avec le graphique"}
          </button>
          <button type="button" onClick={() => inputFichierRef.current.click()} disabled={enCours}>
            Joindre une image
          </button>
        </div>
      </div>
    </div>
  );
}
