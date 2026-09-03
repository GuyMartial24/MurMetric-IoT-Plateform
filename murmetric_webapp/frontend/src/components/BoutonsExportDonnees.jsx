import { useState } from "react";
import { telechargerCSV, telechargerExcel } from "../exportDonnees.js";
import { Button } from "./ui/button.jsx";

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
      <Button type="button" variant="outline" size="sm" onClick={() => telechargerCSV(lignes, `${nomFichier}.csv`)}>
        Export CSV
      </Button>
      <Button type="button" variant="outline" size="sm" onClick={exporterExcel} disabled={enCours}>
        {enCours ? "..." : "Export Excel"}
      </Button>
      {erreur && <span className="text-xs text-destructive">{erreur}</span>}
    </>
  );

  if (imbrique) return boutons;

  return <div className="mt-2 flex items-center gap-2">{boutons}</div>;
}
