import { useState } from "react";
import { canvasVersDataUrl, copierDataUrlDansPressePapiers, svgVersDataUrl, telechargerDataUrl } from "../exportGraphique.js";

// Boutons d'export réutilisables (téléchargement PNG + copie presse-papiers)
// — un seul composant pour les deux types de rendu graphique de l'appli
// (<canvas> pour les nomogrammes, <svg> pour les courbes valeur/temps).
export default function BoutonsExport({ obtenirElement, type, nomFichier = "graphique" }) {
  const [message, setMessage] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const obtenirDataUrl = async () => {
    const el = obtenirElement();
    if (!el) return null;
    return type === "svg" ? await svgVersDataUrl(el) : canvasVersDataUrl(el);
  };

  const telecharger = async () => {
    const url = await obtenirDataUrl();
    if (url) telechargerDataUrl(url, `${nomFichier}.png`);
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
      setMessage("Copie impossible sur ce navigateur — utilise le téléchargement.");
    } finally {
      setEnCours(false);
      setTimeout(() => setMessage(null), 3000);
    }
  };

  return (
    <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginTop: "0.5rem" }}>
      <button type="button" onClick={telecharger}>Télécharger PNG</button>
      <button type="button" onClick={copier} disabled={enCours}>Copier l'image</button>
      {message && <span style={{ color: "#7fd4ff", fontSize: "0.8rem" }}>{message}</span>}
    </div>
  );
}
