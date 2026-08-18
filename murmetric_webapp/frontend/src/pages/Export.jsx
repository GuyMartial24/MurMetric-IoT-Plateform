import { useEffect, useRef, useState } from "react";
import { api } from "../api.js";
import { CANAUX_RETRAIT } from "../nomogrammeAxes.js";

// Export en masse, généré côté serveur (routes /api/export/*) — distinct des
// boutons Export CSV/Excel déjà présents sur chaque courbe (ceux-là
// n'exportent que les points déjà chargés dans le navigateur, adaptés à un
// affichage, pas à un vrai export en masse sur mesures_dewesoft : 10 Hz,
// 1,5 milliard de points, cf. logique_projet.md section 34). Trois onglets,
// un par type de mesure, chacun avec ses propres contraintes.
export default function Export() {
  const [type, setType] = useState("retrait");
  return (
    <div>
      <div className="carte">
        <h2>Export de données</h2>
        <p style={{ color: "#a0a6b5", fontSize: "0.9rem" }}>Génère un fichier côté serveur pour une période choisie.</p>
        <div className="champ" style={{ maxWidth: "260px" }}>
          <label>Type de mesure</label>
          <select value={type} onChange={(e) => setType(e.target.value)}>
            <option value="retrait">Retrait</option>
            <option value="hr_t">Température / Humidité / Point de rosée</option>
            <option value="teneur_eau">Teneur en eau</option>
          </select>
        </div>
      </div>
      {type === "retrait" && <ExportRetrait />}
      {type === "hr_t" && <ExportHrT />}
      {type === "teneur_eau" && <ExportTeneurEau />}
    </div>
  );
}

function SelecteurFormat({ format, onChange }) {
  return (
    <div className="champ">
      <label>Format</label>
      <select value={format} onChange={(e) => onChange(e.target.value)}>
        <option value="csv">CSV</option>
        <option value="parquet">Parquet</option>
      </select>
    </div>
  );
}

function ExportRetrait() {
  const [canauxChoisis, setCanauxChoisis] = useState([...CANAUX_RETRAIT]);
  const [champ, setChamp] = useState("valeur_filtree");
  const [resolution, setResolution] = useState("heure");
  const [format, setFormat] = useState("csv");
  const [mode, setMode] = useState("direct"); // "direct" | "tache"
  const [debut, setDebut] = useState("");
  const [fin, setFin] = useState("");
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);
  const [tache, setTache] = useState(null); // {tache_id, statut, jours_traites, jours_total}
  const intervalleRef = useRef(null);

  const basculerCanal = (canal) =>
    setCanauxChoisis((c) => (c.includes(canal) ? c.filter((x) => x !== canal) : [...c, canal]));

  useEffect(() => {
    // Arrête le sondage si le composant se démonte (changement d'onglet).
    return () => clearInterval(intervalleRef.current);
  }, []);

  const sonderTache = (tacheId) => {
    intervalleRef.current = setInterval(async () => {
      try {
        const etat = await api.etatTacheRetrait(tacheId);
        setTache({ tache_id: tacheId, ...etat });
        if (etat.statut !== "en_cours") clearInterval(intervalleRef.current);
      } catch (e) {
        setErreur(e.message);
        clearInterval(intervalleRef.current);
      }
    }, 2000);
  };

  const exporter = async () => {
    setErreur(null);
    setEnCours(true);
    setTache(null);
    try {
      if (mode === "direct") {
        await api.exporterRetrait({ canaux: canauxChoisis.join(","), champ, debut, fin, resolution, format });
      } else {
        const { tache_id } = await api.demarrerTacheRetrait({
          canaux: canauxChoisis.join(","),
          champ,
          ...(debut ? { debut } : {}),
          ...(fin ? { fin } : {}),
          resolution,
          format,
        });
        setTache({ tache_id, statut: "en_cours", jours_traites: 0, jours_total: 1 });
        sonderTache(tache_id);
      }
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  const telechargerTache = async () => {
    setErreur(null);
    try {
      await api.telechargerTacheRetrait(tache.tache_id);
      // Le fichier est supprimé du serveur juste après l'envoi (rien ne doit
      // persister sur le VPS) : reflète l'état côté client sans attendre un
      // nouveau sondage, qui de toute façon s'est arrêté dès "termine".
      setTache((t) => (t ? { ...t, statut: "telecharge" } : t));
    } catch (e) {
      setErreur(e.message);
    }
  };

  return (
    <div className="carte">
      <h3 style={{ marginTop: 0 }}>Retrait — un fichier, une colonne par canal</h3>
      <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
        Les canaux retrait partagent tous la même grille d'horodatages (DeweSoft les enregistre simultanément) : le
        fichier généré a une ligne par instant mesuré, une colonne par canal choisi.
      </p>
      <div className="champ">
        <label>Canaux</label>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.6rem" }}>
          {CANAUX_RETRAIT.map((canal) => (
            <label key={canal} style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
              <input type="checkbox" checked={canauxChoisis.includes(canal)} onChange={() => basculerCanal(canal)} />
              {canal}
            </label>
          ))}
        </div>
      </div>
      <div className="selection-form" style={{ marginTop: "0.75rem" }}>
        <div className="champ">
          <label>Grandeur</label>
          <select value={champ} onChange={(e) => setChamp(e.target.value)}>
            <option value="valeur_filtree">Retrait filtré</option>
            <option value="valeur">Retrait brut</option>
          </select>
        </div>
        <div className="champ">
          <label>Résolution</label>
          <select value={resolution} onChange={(e) => setResolution(e.target.value)}>
            <option value="heure">Moyenne horaire</option>
            <option value="jour">Moyenne journalière</option>
            <option value="brut">Points bruts (10 Hz)</option>
          </select>
        </div>
        <SelecteurFormat format={format} onChange={setFormat} />
        <div className="champ">
          <label>Début{mode === "tache" ? " (vide = tout l'historique)" : ""}</label>
          <input type="date" value={debut} onChange={(e) => setDebut(e.target.value)} />
        </div>
        <div className="champ">
          <label>Fin{mode === "tache" ? " (vide = maintenant)" : ""}</label>
          <input type="date" value={fin} onChange={(e) => setFin(e.target.value)} />
        </div>
      </div>
      <div className="champ" style={{ marginTop: "0.75rem" }}>
        <label>Livraison</label>
        <div style={{ display: "flex", gap: "1rem" }}>
          <label style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
            <input type="radio" checked={mode === "direct"} onChange={() => setMode("direct")} />
            Téléchargement direct
          </label>
          <label style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
            <input type="radio" checked={mode === "tache"} onChange={() => setMode("tache")} />
            Tâche de fond (période longue, ou tout l'historique)
          </label>
        </div>
      </div>
      {mode === "direct" && (
        <p style={{ color: "#a0a6b5", fontSize: "0.85rem", marginTop: "0.4rem" }}>
          Adapté à une période raisonnable — la réponse arrive au fur et à mesure, sans limite stricte, mais reste une
          requête classique (l'onglet doit rester ouvert).
        </p>
      )}
      {mode === "tache" && (
        <p style={{ color: "#a0a6b5", fontSize: "0.85rem", marginTop: "0.4rem" }}>
          Génère le fichier en arrière-plan sur le serveur — vous pouvez fermer cet onglet et revenir plus tard
          consulter l'avancement, le téléchargement se fait une fois le fichier prêt.
        </p>
      )}
      {erreur && <p className="erreur">{erreur}</p>}
      <button
        onClick={exporter}
        disabled={enCours || canauxChoisis.length === 0 || (mode === "direct" && (!debut || !fin))}
        style={{ marginTop: "0.75rem" }}
      >
        {enCours ? "..." : mode === "direct" ? "Générer l'export" : "Démarrer la tâche"}
      </button>
      {tache && (
        <div style={{ marginTop: "0.75rem" }}>
          {tache.statut === "en_cours" && (
            <p>
              En cours — {tache.jours_traites} / {tache.jours_total} jour(s) traité(s)...
            </p>
          )}
          {tache.statut === "termine" && (
            <p>
              Terminé ({tache.jours_total} jour(s)) — <button onClick={telechargerTache}>Télécharger</button>
            </p>
          )}
          {tache.statut === "telecharge" && (
            <p style={{ color: "#a0a6b5" }}>
              Téléchargé — le fichier a été supprimé du serveur (rien ne persiste sur le VPS). Relancez une tâche pour
              l'obtenir à nouveau.
            </p>
          )}
          {tache.statut === "erreur" && <p className="erreur">Échec : {tache.erreur}</p>}
        </div>
      )}
    </div>
  );
}

function ExportHrT() {
  const [combinaisons, setCombinaisons] = useState([]);
  const [mur, setMur] = useState("");
  const [couche, setCouche] = useState("");
  const [champsChoisis, setChampsChoisis] = useState(["temperature", "humidite", "point_de_rosee"]);
  const [format, setFormat] = useState("csv");
  const [debut, setDebut] = useState("");
  const [fin, setFin] = useState("");
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);

  useEffect(() => {
    api
      .mesuresValeursTags({ type: "hr_t" })
      .then((r) => setCombinaisons(r.combinaisons))
      .catch(() => setCombinaisons([]));
  }, []);

  const murs = [...new Set(combinaisons.map((c) => c.nom_mur).filter(Boolean))];
  const couches = [...new Set(combinaisons.map((c) => c.nom_couche).filter(Boolean))];

  const basculerChamp = (champ) =>
    setChampsChoisis((c) => (c.includes(champ) ? c.filter((x) => x !== champ) : [...c, champ]));

  const exporter = async () => {
    setErreur(null);
    setEnCours(true);
    try {
      await api.exporterHrT({
        ...(mur ? { mur } : {}),
        ...(couche ? { couche } : {}),
        champs: champsChoisis.join(","),
        debut,
        fin,
        format,
      });
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <div className="carte">
      <h3 style={{ marginTop: 0 }}>
        Température / Humidité / Point de rosée — un fichier, une ligne par capteur/instant
      </h3>
      <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
        Les capteurs BLE ne partagent pas d'horodatage commun (chacun logue à son propre rythme) — contrairement au
        retrait, le fichier reste au format long (une colonne "capteur" identifie chaque ligne).
      </p>
      <div className="champ">
        <label>Grandeurs</label>
        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.6rem" }}>
          {[
            { valeur: "temperature", label: "Température" },
            { valeur: "humidite", label: "Humidité" },
            { valeur: "point_de_rosee", label: "Point de rosée" },
          ].map(({ valeur, label }) => (
            <label key={valeur} style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
              <input type="checkbox" checked={champsChoisis.includes(valeur)} onChange={() => basculerChamp(valeur)} />
              {label}
            </label>
          ))}
        </div>
      </div>
      <div className="selection-form" style={{ marginTop: "0.75rem" }}>
        <div className="champ">
          <label>Mur</label>
          <select value={mur} onChange={(e) => setMur(e.target.value)}>
            <option value="">— tous —</option>
            {murs.map((m) => (
              <option key={m} value={m}>
                {m}
              </option>
            ))}
          </select>
        </div>
        <div className="champ">
          <label>Couche</label>
          <select value={couche} onChange={(e) => setCouche(e.target.value)}>
            <option value="">— toutes —</option>
            {couches.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </div>
        <SelecteurFormat format={format} onChange={setFormat} />
        <div className="champ">
          <label>Début</label>
          <input type="date" value={debut} onChange={(e) => setDebut(e.target.value)} />
        </div>
        <div className="champ">
          <label>Fin</label>
          <input type="date" value={fin} onChange={(e) => setFin(e.target.value)} />
        </div>
      </div>
      {erreur && <p className="erreur">{erreur}</p>}
      <button
        onClick={exporter}
        disabled={enCours || champsChoisis.length === 0 || !debut || !fin}
        style={{ marginTop: "0.75rem" }}
      >
        {enCours ? "Génération..." : "Générer l'export"}
      </button>
    </div>
  );
}

function ExportTeneurEau() {
  const [format, setFormat] = useState("csv");
  const [debut, setDebut] = useState("");
  const [fin, setFin] = useState("");
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);

  const exporter = async () => {
    setErreur(null);
    setEnCours(true);
    try {
      await api.exporterTeneurEau({ ...(debut ? { debut } : {}), ...(fin ? { fin } : {}), format });
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <div className="carte">
      <h3 style={{ marginTop: 0 }}>Teneur en eau — toutes les saisies</h3>
      <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
        Relevés ponctuels, volume négligeable — laisser Début/Fin vides pour tout exporter.
      </p>
      <div className="selection-form">
        <SelecteurFormat format={format} onChange={setFormat} />
        <div className="champ">
          <label>Début</label>
          <input type="date" value={debut} onChange={(e) => setDebut(e.target.value)} />
        </div>
        <div className="champ">
          <label>Fin</label>
          <input type="date" value={fin} onChange={(e) => setFin(e.target.value)} />
        </div>
      </div>
      {erreur && <p className="erreur">{erreur}</p>}
      <button onClick={exporter} disabled={enCours} style={{ marginTop: "0.75rem" }}>
        {enCours ? "Génération..." : "Générer l'export"}
      </button>
    </div>
  );
}
