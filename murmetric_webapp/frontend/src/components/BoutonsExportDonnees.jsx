import { useState } from "react";
import { telechargerCSV, telechargerExcel } from "../exportDonnees.js";

export default function BoutonsExportDonnees({ lignes, nomFichier = "donnees", imbrique = false }) {
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);

  if (!lignes || lignes.length === 0) return null;

  const exporterExcel = async () => {
    setEnCours(true);
    setErreur(null);
    try {
      await telechargerExcel(lignes, `${nomFichier}.xlsx`);
    } catch {
      setErreur("Export Excel impossible — réessaie ou utilise l'export CSV.");
    } finally {
      setEnCours(false);
    }
  };

  const boutons = (
    <>
      <button type="button" onClick={() => telechargerCSV(lignes, `${nomFichier}.csv`)}>
        Export CSV
      </button>
      <button type="button" onClick={exporterExcel} disabled={enCours}>
        {enCours ? "..." : "Export Excel"}
      </button>
      {erreur && <span style={{ color: "#ff8080", fontSize: "0.8rem" }}>{erreur}</span>}
    </>
  );

  if (imbrique) return boutons;

  return <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginTop: "0.5rem" }}>{boutons}</div>;
}
