import { useEffect, useState } from "react";
import { api } from "../api.js";
import BoutonsExportDonnees from "../components/BoutonsExportDonnees.jsx";
import ChampSelectOuAutre from "../components/ChampSelectOuAutre.jsx";
import { useCouchesParMur, useMursCouchesConnus } from "../mursCouches.js";

const VIDE = { mur: "", couche: "", valeur_pourcent: "", commentaire: "", date_mesure: "" };

export default function TeneurEau() {
  const [liste, setListe] = useState([]);
  const [saisie, setSaisie] = useState(VIDE);
  const [enEdition, setEnEdition] = useState(null); // { original, valeurs }
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);
  const [filtreMur, setFiltreMur] = useState("");
  const [filtreCouche, setFiltreCouche] = useState("");
  const { murs: mursConnus } = useMursCouchesConnus();
  // Couches filtrées par mur + type teneur_eau (31/08/2026, demande
  // explicite) — 2 listes indépendantes, la saisie et l'édition en ligne ne
  // portant pas forcément sur le même mur au même instant.
  const couchesPourSaisie = useCouchesParMur("teneur_eau", saisie.mur);
  const couchesPourEdition = useCouchesParMur("teneur_eau", enEdition?.valeurs?.mur);

  const charger = async () => {
    try {
      setListe(await api.listerTeneurEau());
    } catch (e) {
      setErreur(e.message);
    }
  };

  useEffect(() => {
    charger();
  }, []);

  const soumettre = async (e) => {
    e.preventDefault();
    setEnCours(true);
    setErreur(null);
    try {
      await api.creerTeneurEau({
        ...saisie,
        valeur_pourcent: Number(saisie.valeur_pourcent),
        date_mesure: saisie.date_mesure ? new Date(saisie.date_mesure).toISOString() : null,
      });
      setSaisie(VIDE);
      await charger();
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  const demarrerEdition = (ligne) => {
    setEnEdition({
      original: { mur: ligne.mur, couche: ligne.couche, date_mesure: ligne.time },
      valeurs: {
        mur: ligne.mur,
        couche: ligne.couche,
        valeur_pourcent: ligne.value,
        commentaire: ligne.commentaire || "",
        date_mesure: ligne.time,
      },
    });
  };

  const enregistrerCorrection = async () => {
    setErreur(null);
    try {
      await api.corrigerTeneurEau({
        mur_original: enEdition.original.mur,
        couche_original: enEdition.original.couche,
        date_mesure_original: enEdition.original.date_mesure,
        mur: enEdition.valeurs.mur,
        couche: enEdition.valeurs.couche,
        valeur_pourcent: Number(enEdition.valeurs.valeur_pourcent),
        commentaire: enEdition.valeurs.commentaire,
        date_mesure: enEdition.valeurs.date_mesure,
      });
      setEnEdition(null);
      await charger();
    } catch (e) {
      setErreur(e.message);
    }
  };

  // Une entrée = 2 lignes brutes InfluxDB (field teneur_eau_pourcent +
  // field commentaire, même mesure/tags/heure) — regroupées ici par clé
  // (mur, couche, time) pour l'affichage.
  const groupes = Object.values(
    liste.reduce((acc, p) => {
      const cle = `${p.mur}|${p.couche}|${p.time}`;
      acc[cle] = acc[cle] || { mur: p.mur, couche: p.couche, time: p.time };
      if (p.field === "teneur_eau_pourcent") acc[cle].value = p.value;
      if (p.field === "commentaire") acc[cle].commentaire = p.value;
      return acc;
    }, {}),
  );

  // Filtres Mur/Couche (27/08/2026) — indépendants l'un de l'autre, menus
  // dérivés des valeurs déjà présentes plutôt qu'une saisie libre.
  const mursDisponibles = [...new Set(groupes.map((g) => g.mur))].sort((a, b) => a.localeCompare(b));
  const couchesDisponibles = [...new Set(groupes.map((g) => g.couche))].sort((a, b) => a.localeCompare(b));
  const groupesFiltres = groupes.filter(
    (g) => (!filtreMur || g.mur === filtreMur) && (!filtreCouche || g.couche === filtreCouche),
  );

  return (
    <div>
      <div className="carte">
        <h2>Nouvelle saisie — teneur en eau</h2>
        <form onSubmit={soumettre} className="selection-form">
          <div className="champ">
            <label>Mur</label>
            <ChampSelectOuAutre
              required
              valeur={saisie.mur}
              options={mursConnus}
              onChange={(v) => setSaisie({ ...saisie, mur: v })}
              placeholder="SOCMA 1"
            />
          </div>
          <div className="champ">
            <label>Couche</label>
            <ChampSelectOuAutre
              required
              valeur={saisie.couche}
              options={couchesPourSaisie}
              onChange={(v) => setSaisie({ ...saisie, couche: v })}
              placeholder="interface carreau et exterieur"
            />
          </div>
          <div className="champ">
            <label>Valeur (%)</label>
            <input
              required
              type="number"
              step="0.01"
              value={saisie.valeur_pourcent}
              onChange={(e) => setSaisie({ ...saisie, valeur_pourcent: e.target.value })}
            />
          </div>
          <div className="champ">
            <label>Date de mesure</label>
            <input
              type="datetime-local"
              value={saisie.date_mesure}
              onChange={(e) => setSaisie({ ...saisie, date_mesure: e.target.value })}
            />
          </div>
          <div className="champ" style={{ gridColumn: "1 / -1" }}>
            <label>Commentaire</label>
            <input value={saisie.commentaire} onChange={(e) => setSaisie({ ...saisie, commentaire: e.target.value })} />
          </div>
        </form>
        <button onClick={soumettre} disabled={enCours}>
          {enCours ? "Envoi..." : "Enregistrer"}
        </button>
        {erreur && <p className="erreur">{erreur}</p>}
      </div>

      <div className="carte">
        <h2>
          Saisies existantes ({groupesFiltres.length}
          {groupesFiltres.length !== groupes.length ? ` / ${groupes.length}` : ""})
        </h2>
        <BoutonsExportDonnees lignes={groupesFiltres} nomFichier="teneur_eau" />
        <div className="tableau-scroll">
          <table>
            <thead>
              <tr>
                <th>Mur</th>
                <th>Couche</th>
                <th>Valeur (%)</th>
                <th>Date</th>
                <th>Commentaire</th>
                <th></th>
              </tr>
              <tr>
                <th>
                  <select value={filtreMur} onChange={(e) => setFiltreMur(e.target.value)}>
                    <option value="">Tous</option>
                    {mursDisponibles.map((m) => (
                      <option key={m} value={m}>
                        {m}
                      </option>
                    ))}
                  </select>
                </th>
                <th>
                  <select value={filtreCouche} onChange={(e) => setFiltreCouche(e.target.value)}>
                    <option value="">Toutes</option>
                    {couchesDisponibles.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                </th>
                <th></th>
                <th></th>
                <th></th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {groupesFiltres.map((g) => (
                <tr key={`${g.mur}|${g.couche}|${g.time}`}>
                  {enEdition?.original.mur === g.mur &&
                  enEdition.original.couche === g.couche &&
                  enEdition.original.date_mesure === g.time ? (
                    <>
                      <td>
                        <ChampSelectOuAutre
                          valeur={enEdition.valeurs.mur}
                          options={mursConnus}
                          onChange={(v) => setEnEdition({ ...enEdition, valeurs: { ...enEdition.valeurs, mur: v } })}
                        />
                      </td>
                      <td>
                        <ChampSelectOuAutre
                          valeur={enEdition.valeurs.couche}
                          options={couchesPourEdition}
                          onChange={(v) => setEnEdition({ ...enEdition, valeurs: { ...enEdition.valeurs, couche: v } })}
                        />
                      </td>
                      <td>
                        <input
                          type="number"
                          step="0.01"
                          value={enEdition.valeurs.valeur_pourcent}
                          onChange={(e) =>
                            setEnEdition({
                              ...enEdition,
                              valeurs: { ...enEdition.valeurs, valeur_pourcent: e.target.value },
                            })
                          }
                        />
                      </td>
                      <td colSpan={2}>
                        <input
                          value={enEdition.valeurs.commentaire}
                          onChange={(e) =>
                            setEnEdition({
                              ...enEdition,
                              valeurs: { ...enEdition.valeurs, commentaire: e.target.value },
                            })
                          }
                        />
                      </td>
                      <td>
                        <button onClick={enregistrerCorrection}>Enregistrer</button>{" "}
                        <button onClick={() => setEnEdition(null)}>Annuler</button>
                      </td>
                    </>
                  ) : (
                    <>
                      <td>{g.mur}</td>
                      <td>{g.couche}</td>
                      <td>{g.value?.toFixed(2)}</td>
                      <td>{new Date(g.time).toLocaleString("fr-FR")}</td>
                      <td>{g.commentaire}</td>
                      <td>
                        <button onClick={() => demarrerEdition(g)}>Éditer</button>
                      </td>
                    </>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
