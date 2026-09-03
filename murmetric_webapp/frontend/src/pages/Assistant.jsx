import { useRef, useState } from "react";
import { api } from "../api.js";
import GraphiqueSVG from "../components/GraphiqueSVG.jsx";
import SelecteurMesure from "../components/SelecteurMesure.jsx";
import { Button } from "../components/ui/button.jsx";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card.jsx";
import { Label } from "../components/ui/label.jsx";
import { Textarea } from "../components/ui/textarea.jsx";
import { useEtatAssistant } from "../EtatPagesContext.jsx";
import { svgVersDataUrl } from "../exportGraphique.js";
import { classesChampNatif } from "../lib/utils.js";
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
      <p key={i} className={estNote ? "text-sm text-muted-foreground italic" : undefined}>
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
      <Card>
        <CardHeader>
          <CardTitle>Assistant IA</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <p className="text-sm text-muted-foreground">
            Ancré sur la sélection ci-dessous — jamais sur des points bruts, uniquement sur des statistiques
            pré-agrégées, sauf si tu joins une image (le graphique de l'appli, ou une capture collée/importée) : l'IA
            l'analyse alors directement. Les brouillons de rapport sont à relire avant usage.
          </p>
          <SelecteurMesure valeur={selection} onChange={setSelection} />
          <div className="flex max-w-[260px] flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Mode</Label>
            <select value={mode} onChange={(e) => setMode(e.target.value)} className={classesChampNatif}>
              <option value="explain">Explication de la courbe</option>
              <option value="report">Brouillon de rapport d'instrumentation</option>
            </select>
          </div>
          <Button onClick={chargerCourbe} disabled={enCoursCourbe} className="self-start">
            {enCoursCourbe ? "Chargement..." : "Charger la courbe (pour l'analyse visuelle, optionnel)"}
          </Button>
        </CardContent>
      </Card>

      {points && points.length > 0 && (
        <Card className="mt-5">
          <CardHeader>
            <CardTitle>Courbe — {libelleGrandeur(`${selection.type}:${selection.champ}`)}</CardTitle>
          </CardHeader>
          <CardContent>
            <GraphiqueSVG ref={graphiqueRef} points={points} champ={selection.champ} />
          </CardContent>
        </Card>
      )}
      {points && points.length === 0 && (
        <Card className="mt-5">
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Aucune donnée pour cette sélection sur la période choisie — essaie d'élargir la période, ou vérifie le
              mur/la couche (la dernière mesure disponible peut être plus ancienne que la période demandée).
            </p>
          </CardContent>
        </Card>
      )}

      <Card className="mt-5">
        <CardContent>
          {historique.map((m, i) => (
            <div key={i} className={`chat-message ${m.role}`}>
              {m.etiquette && (
                <span className="mr-1.5 inline-block rounded border border-border px-1.5 text-xs text-muted-foreground">
                  {m.etiquette}
                </span>
              )}
              {m.role === "assistant" ? <TexteAssistant texte={m.texte} /> : m.texte}
            </div>
          ))}
          {erreur && (
            <p className="text-sm text-destructive">
              {erreur}
              {dernierEchec && (
                <Button variant="outline" size="sm" onClick={reessayer} disabled={enCours} className="ml-2.5">
                  Réessayer
                </Button>
              )}
            </p>
          )}
          {imageJointe && (
            <div className="mb-2 flex items-center gap-2.5">
              <img src={imageJointe} alt="Image jointe" className="max-h-[70px] rounded border border-border" />
              <span className="text-xs text-muted-foreground">
                Image jointe — sera envoyée avec le prochain message.
              </span>
              <Button variant="ghost" size="sm" onClick={() => setImageJointe(null)}>
                Retirer
              </Button>
            </div>
          )}
          <input
            type="file"
            accept="image/*"
            ref={inputFichierRef}
            className="hidden"
            onChange={(e) => {
              attacherImage(e.target.files[0]);
              e.target.value = "";
            }}
          />
          <Textarea
            rows={3}
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            onPaste={collerImage}
            placeholder="ex. Explique l'évolution de la température sur cette sélection (Ctrl+V pour coller une capture)"
          />
          <div className="mt-2 flex flex-wrap gap-2">
            <Button onClick={() => envoyer(imageJointe ? "image" : "texte")} disabled={enCours}>
              {enCours ? "Réflexion..." : "Envoyer"}
            </Button>
            <Button
              variant="outline"
              onClick={() => envoyer("graphique")}
              disabled={enCours || !points || points.length === 0}
              title={
                !points || points.length === 0
                  ? "Charge d'abord la courbe ci-dessus"
                  : "Envoie l'image du graphique à l'IA (analyse visuelle)"
              }
            >
              {enCours ? "Réflexion..." : "Envoyer avec le graphique"}
            </Button>
            <Button variant="outline" type="button" onClick={() => inputFichierRef.current.click()} disabled={enCours}>
              Joindre une image
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
