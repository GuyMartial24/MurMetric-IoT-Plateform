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
        colonnes={[
          "nom",
          "famille_capteur",
          "nom_mur",
          "nom_couche",
          "ingestion",
          "derniere_detection",
          "dernier_rssi",
          "derniere_batterie",
        ]}
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
        avertissementEdition={
          "Cette modification ne s'appliquera qu'aux nouvelles mesures — " +
          "l'historique déjà enregistré gardera l'ancien étiquetage. Pour " +
          "retrouver l'historique complet d'un canal malgré un renommage, " +
          "utilisez le sélecteur « Canal » plutôt que Mur/Couche."
        }
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
  derniere_detection: "Dernière détection",
  dernier_rssi: "RSSI",
  derniere_batterie: "Batterie",
};

// Télémétrie (dernière détection/RSSI/batterie, 19/08/2026) : envoyée par le
// script d'ingestion du Pi indépendamment du flag ingestion (utile pour
// surveiller un capteur pas encore activé, ou anticiper une perte de signal
// — capteurs noyés dans les parois, impossibles à vérifier physiquement).
// Seuils calés sur l'intervalle d'envoi throttlé côté Pi (5 min) : un
// capteur sain devrait quasi-systématiquement apparaître "récent".
function etatDerniereDetection(iso) {
  if (!iso) return { texte: "Jamais vu", etat: "neutre" };
  const minutes = (Date.now() - new Date(iso).getTime()) / 60000;
  const texte =
    minutes < 60
      ? `il y a ${Math.round(minutes)} min`
      : minutes < 1440
        ? `il y a ${Math.round(minutes / 60)} h`
        : `il y a ${Math.round(minutes / 1440)} j`;
  const etat = minutes < 15 ? "ok" : minutes < 180 ? "attention" : "erreur";
  return { texte, etat };
}

// Blue Maestro transmet un pourcentage en continu (absent = jamais reçu).
// ELA (mode Service Data, 19/08/2026) ne transmet CE champ que lorsque sa
// batterie réelle est déjà sous 15% (cf. logique_projet.md section 40,
// addendum) — donc pour ce capteur, "absent" ne veut pas dire "jamais reçu"
// mais "rien à signaler", à condition d'avoir déjà reçu au moins une
// détection (sinon on ne sait vraiment rien, cf. dernier paramètre).
function etatBatterie(pourcentage, familleCapteur, derniereDetection) {
  if (pourcentage !== undefined && pourcentage !== null) {
    const etat = pourcentage < 15 ? "erreur" : pourcentage < 30 ? "attention" : "ok";
    return { texte: `${pourcentage} %`, etat };
  }
  if (familleCapteur === "ela" && derniereDetection) {
    return { texte: "Saine (> 15 %)", etat: "ok" };
  }
  return null;
}

function TableauCapteurs({
  titre,
  lignes,
  colonnes,
  champsEditables,
  cleColonne,
  enregistrer,
  recharger,
  nomFichierExport,
  avertissementEdition,
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
      {enEdition && avertissementEdition && <p className="avertissement">{avertissementEdition}</p>}
      <div className="tableau-scroll">
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
                      ) : champ === "derniere_detection" ? (
                        (() => {
                          const { texte, etat } = etatDerniereDetection(c.derniere_detection);
                          return <Pastille etat={etat} texte={texte} />;
                        })()
                      ) : champ === "derniere_batterie" ? (
                        (() => {
                          const info = etatBatterie(c.derniere_batterie, c.famille_capteur, c.derniere_detection);
                          return info ? <Pastille etat={info.etat} texte={info.texte} /> : "—";
                        })()
                      ) : champ === "dernier_rssi" ? (
                        c.dernier_rssi !== undefined && c.dernier_rssi !== null ? (
                          `${c.dernier_rssi} dBm`
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
    </div>
  );
}
