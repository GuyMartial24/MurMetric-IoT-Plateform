import { useState } from "react";
import { canvasVersDataUrl, copierDataUrlDansPressePapiers, svgVersDataUrl } from "../exportGraphique.js";

// Bouton d'export réutilisable (copie presse-papiers) — un seul composant
// pour les deux types de rendu graphique de l'appli (<canvas> pour les
// nomogrammes, <svg> pour les courbes valeur/temps). Le téléchargement PNG
// a été retiré le 28/08/2026 (demande explicite) — la copie presse-papiers
// couvre le même besoin (coller dans un document/message) sans passer par
// le système de fichiers.
export default function BoutonsExport({ obtenirElement, type, imbrique = false }) {
  const [message, setMessage] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const obtenirDataUrl = async () => {
    const el = obtenirElement();
    if (!el) return null;
    return type === "svg" ? await svgVersDataUrl(el) : canvasVersDataUrl(el);
  };

  const copier = async () => {
    setEnCours(true);
    setMessage(null);
    try {
      const url = await obtenirDataUrl();
      if (url) {
        await copierDataUrlDansPressePapiers(url);
        setMessage("Copié dans le presse-papiers ✓");
      }
    } catch {
      setMessage("Copie impossible sur ce navigateur.");
    } finally {
      setEnCours(false);
      setTimeout(() => setMessage(null), 3000);
    }
  };

  const boutons = (
    <>
      <button type="button" onClick={copier} disabled={enCours}>
        Copier l'image
      </button>
      {message && <span style={{ color: "#7fd4ff", fontSize: "0.8rem" }}>{message}</span>}
    </>
  );

  if (imbrique) return boutons;

  return <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginTop: "0.5rem" }}>{boutons}</div>;
}
