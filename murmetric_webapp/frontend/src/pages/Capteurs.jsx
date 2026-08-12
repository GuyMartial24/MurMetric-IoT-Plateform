import { useEffect, useState } from "react";
import { api } from "../api.js";

// Lecture seule en V1 — cf. logique_projet.md section 32 : l'écriture depuis
// l'appli est repoussée tant que capteurs.json/capteurs_retrait.json ne sont
// pas une source de configuration concurrente-safe.
export default function Capteurs() {
  const [hrT, setHrT] = useState(null);
  const [retrait, setRetrait] = useState(null);
  const [erreur, setErreur] = useState(null);

  useEffect(() => {
    Promise.all([api.capteursHrT(), api.capteursRetrait()])
      .then(([h, r]) => {
        setHrT(h);
        setRetrait(r);
      })
      .catch((e) => setErreur(e.message));
  }, []);

  const lignes = (donnees) =>
    donnees
      ? Object.entries(donnees).filter(([cle]) => cle !== "_schema")
      : [];

  return (
    <div>
      {erreur && <p className="erreur">{erreur}</p>}

      <div className="carte">
        <h2>Capteurs HR/T ({lignes(hrT).length})</h2>
        <table>
          <thead>
            <tr><th>MAC / clé</th><th>Nom</th><th>Famille</th><th>Mur</th><th>Couche</th><th>Ingestion</th></tr>
          </thead>
          <tbody>
            {lignes(hrT).map(([cle, c]) => (
              <tr key={cle}>
                <td>{cle}</td>
                <td>{c.nom}</td>
                <td>{c.famille_capteur}</td>
                <td>{c.nom_mur}</td>
                <td>{c.nom_couche}</td>
                <td>{c.ingestion ? "✅" : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="carte">
        <h2>Canaux retrait ({lignes(retrait).length})</h2>
        <table>
          <thead>
            <tr><th>Canal</th><th>Mur</th><th>Couche</th><th>Position</th><th>Ingestion</th></tr>
          </thead>
          <tbody>
            {lignes(retrait).map(([cle, c]) => (
              <tr key={cle}>
                <td>{cle}</td>
                <td>{c.nom_mur}</td>
                <td>{c.nom_couche}</td>
                <td>{c.position}</td>
                <td>{c.ingestion ? "✅" : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
