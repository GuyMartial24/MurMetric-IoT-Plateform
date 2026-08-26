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
const INTERVALLE_RAFRAICHISSEMENT_MS = 30_000; // même cadence que Monitoring.jsx

export default function Capteurs() {
  const [hrT, setHrT] = useState(null);
  const [retrait, setRetrait] = useState(null);
  const [erreur, setErreur] = useState(null);

  const charger = () =>
    Promise.all([api.capteursHrT(), api.capteursRetrait(), api.dernieresMesuresHrT()])
      .then(([h, r, mesures]) => {
        setHrT(
          Object.fromEntries(
            Object.entries(h).map(([cle, c]) =>
              cle === "_schema" ? [cle, c] : [cle, { ...c, derniere_mesure: mesures[cle] }],
            ),
          ),
        );
        setRetrait(r);
      })
      .catch((e) => setErreur(e.message));

  useEffect(() => {
    charger();
    const id = setInterval(charger, INTERVALLE_RAFRAICHISSEMENT_MS);
    return () => clearInterval(id);
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
          "derniere_mesure",
          "lint_cible_s",
        ]}
        champsEditables={["nom", "nom_mur", "nom_couche", "ingestion", "lint_cible_s"]}
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
  dernier_rssi: "RSSI (dBm)",
  derniere_batterie: "Batterie",
  derniere_mesure: "Dernière mesure",
  lint_cible_s: "Intervalle mesure",
};

// Intervalle de mesure Blue Maestro (26/08/2026) — réglable par capteur,
// GATT via configure_capteurs.py sur le Pi (cf. logique_projet.md). Non
// applicable à ELA : configuration NFC uniquement, aucune commande à
// distance possible (encore moins une fois le capteur noyé dans le mur).
function formatDuree(s) {
  if (s == null) return "?";
  if (s % 86400 === 0) return `${s / 86400} j`;
  if (s % 3600 === 0) return `${s / 3600} h`;
  if (s % 60 === 0) return `${s / 60} min`;
  return `${s} s`;
}

// Valeurs proposées pour lint_cible_s (26/08/2026) — menu déroulant plutôt
// qu'un champ en secondes brutes, plus lisible et évite les erreurs de
// saisie (ex. "864000" au lieu de "86400"). Bornes matérielles Blue
// Maestro : 1 s - 86 400 s (24 h), cf. configure_capteurs.py.
const PRESETS_LINT_CIBLE_S = [
  { s: 60, libelle: "1 min" },
  { s: 120, libelle: "2 min" },
  { s: 180, libelle: "3 min" },
  { s: 240, libelle: "4 min" },
  { s: 300, libelle: "5 min" },
  { s: 900, libelle: "15 min" },
  { s: 1800, libelle: "30 min" },
  { s: 3600, libelle: "1 h" },
  { s: 7200, libelle: "2 h" },
  { s: 10800, libelle: "3 h" },
  { s: 14400, libelle: "4 h" },
  { s: 21600, libelle: "6 h" },
  { s: 28800, libelle: "8 h" },
  { s: 43200, libelle: "12 h" },
  { s: 86400, libelle: "24 h" },
];

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

// Dernière mesure réellement écrite dans InfluxDB (température/humidité,
// 26/08/2026) — distincte de la télémétrie détection/RSSI/batterie
// ci-dessus, qui ne prouve que la réception du signal BLE, pas qu'une
// mesure ait été publiée (n'existe que pour les capteurs ingestion: true,
// cf. dernieres_mesures_hr_t côté backend). Mêmes seuils de fraîcheur que
// etatDerniereDetection. Date affichée seulement si différente d'aujourd'hui
// (question utilisateur : sans ça, rien ne distingue "aujourd'hui 19:26"
// d'un horodatage vieux de plusieurs jours à la même heure).
function texteMesure(mesure) {
  if (!mesure || mesure.heure == null) return null;
  const parties = [];
  if (mesure.temperature != null) parties.push(`${mesure.temperature.toFixed(1)}°C`);
  if (mesure.humidite != null) parties.push(`${mesure.humidite.toFixed(0)} %`);
  const date = new Date(mesure.heure);
  const aujourdhui = date.toDateString() === new Date().toDateString();
  const heure = aujourdhui
    ? date.toLocaleTimeString("fr-FR")
    : `${date.toLocaleDateString("fr-FR", { day: "2-digit", month: "2-digit" })} ${date.toLocaleTimeString("fr-FR")}`;
  const minutes = (Date.now() - date.getTime()) / 60000;
  const etat = minutes < 15 ? "ok" : minutes < 180 ? "attention" : "erreur";
  return { texte: `${parties.join(" · ")} — ${heure}`, etat };
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
        champsEditables.map((champ) => [
          champ,
          c[champ] ?? (champ === "ingestion" ? false : champ === "lint_cible_s" ? 86400 : ""),
        ]),
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
                        ) : champ === "lint_cible_s" ? (
                          c.famille_capteur === "bluemaestro" ? (
                            <select
                              value={enEdition.valeurs.lint_cible_s}
                              onChange={(e) =>
                                setEnEdition({
                                  ...enEdition,
                                  valeurs: { ...enEdition.valeurs, lint_cible_s: Number(e.target.value) },
                                })
                              }
                            >
                              {PRESETS_LINT_CIBLE_S.map((p) => (
                                <option key={p.s} value={p.s}>
                                  {p.libelle}
                                </option>
                              ))}
                            </select>
                          ) : (
                            "— (NFC uniquement)"
                          )
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
                          c.dernier_rssi
                        ) : (
                          "—"
                        )
                      ) : champ === "lint_cible_s" ? (
                        c.famille_capteur !== "bluemaestro" ? (
                          "—"
                        ) : (
                          (() => {
                            const cible = c.lint_cible_s ?? 86400;
                            const applique = c.lint_max_confirme_s;
                            return applique === cible ? (
                              <Pastille etat="ok" texte={formatDuree(applique)} />
                            ) : (
                              <Pastille
                                etat="attention"
                                texte={`${formatDuree(cible)} (cible${
                                  applique != null ? `, ${formatDuree(applique)} appliqué` : ", en attente"
                                })`}
                              />
                            );
                          })()
                        )
                      ) : champ === "derniere_mesure" ? (
                        (() => {
                          const info = texteMesure(c.derniere_mesure);
                          return info ? <Pastille etat={info.etat} texte={info.texte} /> : "—";
                        })()
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
