import { useEffect, useState } from "react";
import { api } from "../api.js";
import BoutonsExportDonnees from "../components/BoutonsExportDonnees.jsx";
import Pastille from "../components/Pastille.jsx";

// Édition en place (chantier "source unique", section 32, 13/08/2026) :
// capteurs.json/capteurs_retrait.json vivent désormais sur le volume
// persistant de la webapp, qui en est la source de vérité — le PC Amiens et
// le Pi interrogent son API au lieu de leur copie locale, donc une
// modification faite ici a un effet réel sur l'étiquetage des prochaines
// mesures (pas seulement cosmétique comme avant ce chantier).
export default function Capteurs() {
  const [hrT, setHrT] = useState(null);
  const [retrait, setRetrait] = useState(null);
  const [erreur, setErreur] = useState(null);

  const charger = () =>
    Promise.all([api.capteursHrT(), api.capteursRetrait()])
      .then(([h, r]) => {
        setHrT(h);
        setRetrait(r);
      })
      .catch((e) => setErreur(e.message));

  useEffect(() => {
    charger();
  }, []);

  const lignes = (donnees) => (donnees ? Object.entries(donnees).filter(([cle]) => cle !== "_schema") : []);

  return (
    <div>
      {erreur && <p className="erreur">{erreur}</p>}

      <TableauCapteurs
        titre="Capteurs HR/T"
        lignes={lignes(hrT)}
        colonnes={["nom", "famille_capteur", "nom_mur", "nom_couche", "ingestion"]}
        champsEditables={["nom", "nom_mur", "nom_couche", "ingestion"]}
        cleColonne="MAC / clé"
        enregistrer={(cle, champs) => api.modifierCapteurHrT(cle, champs)}
        recharger={charger}
        nomFichierExport="capteurs_hr_t"
      />

      <TableauCapteurs
        titre="Canaux retrait"
        lignes={lignes(retrait)}
        colonnes={["nom_mur", "nom_couche", "position", "ingestion"]}
        champsEditables={["nom_mur", "nom_couche", "position", "ingestion"]}
        cleColonne="Canal"
        enregistrer={(cle, champs) => api.modifierCapteurRetrait(cle, champs)}
        recharger={charger}
        nomFichierExport="capteurs_retrait"
      />
    </div>
  );
}

const LIBELLES = {
  nom: "Nom",
  famille_capteur: "Famille",
  nom_mur: "Mur",
  nom_couche: "Couche",
  position: "Position",
  ingestion: "Ingestion",
};

function TableauCapteurs({
  titre,
  lignes,
  colonnes,
  champsEditables,
  cleColonne,
  enregistrer,
  recharger,
  nomFichierExport,
}) {
  const [enEdition, setEnEdition] = useState(null); // { cle, valeurs }
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);

  const demarrerEdition = (cle, c) => {
    setErreur(null);
    setEnEdition({
      cle,
      valeurs: Object.fromEntries(
        champsEditables.map((champ) => [champ, c[champ] ?? (champ === "ingestion" ? false : "")]),
      ),
    });
  };

  const valider = async () => {
    setEnCours(true);
    setErreur(null);
    try {
      await enregistrer(enEdition.cle, enEdition.valeurs);
      setEnEdition(null);
      await recharger();
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <div className="carte">
      <h2>
        {titre} ({lignes.length})
      </h2>
      <BoutonsExportDonnees
        lignes={lignes.map(([cle, c]) => ({ [cleColonne]: cle, ...c }))}
        nomFichier={nomFichierExport}
      />
      {erreur && <p className="erreur">{erreur}</p>}
      <table>
        <thead>
          <tr>
            <th>{cleColonne}</th>
            {colonnes.map((champ) => (
              <th key={champ}>{LIBELLES[champ]}</th>
            ))}
            <th></th>
          </tr>
        </thead>
        <tbody>
          {lignes.map(([cle, c]) => {
            const edition = enEdition?.cle === cle;
            return (
              <tr key={cle}>
                <td>{cle}</td>
                {colonnes.map((champ) => (
                  <td key={champ}>
                    {edition && champsEditables.includes(champ) ? (
                      champ === "ingestion" ? (
                        <input
                          type="checkbox"
                          checked={enEdition.valeurs.ingestion}
                          onChange={(e) =>
                            setEnEdition({
                              ...enEdition,
                              valeurs: { ...enEdition.valeurs, ingestion: e.target.checked },
                            })
                          }
                        />
                      ) : (
                        <input
                          value={enEdition.valeurs[champ]}
                          onChange={(e) =>
                            setEnEdition({ ...enEdition, valeurs: { ...enEdition.valeurs, [champ]: e.target.value } })
                          }
                        />
                      )
                    ) : champ === "ingestion" ? (
                      c.ingestion ? (
                        <Pastille etat="ok" texte="Oui" />
                      ) : (
                        "—"
                      )
                    ) : (
                      c[champ]
                    )}
                  </td>
                ))}
                <td>
                  {edition ? (
                    <>
                      <button onClick={valider} disabled={enCours}>
                        {enCours ? "..." : "Enregistrer"}
                      </button>{" "}
                      <button onClick={() => setEnEdition(null)} disabled={enCours}>
                        Annuler
                      </button>
                    </>
                  ) : (
                    <button onClick={() => demarrerEdition(cle, c)}>Éditer</button>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
