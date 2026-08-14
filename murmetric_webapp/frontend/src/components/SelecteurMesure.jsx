import { useEffect, useState } from "react";
import { api } from "../api.js";
import { GRANDEURS_MESURABLES } from "../nomogrammeAxes.js";

// Sélection partagée par la Vue d'ensemble et l'Assistant IA — même forme
// que le modèle `Selection` côté backend (app/routers/assistant.py).
//
// Le picker "Grandeur" expose les mêmes éléments que les axes du
// nomogramme (température/humidité/point de rosée/retrait brut-filtré/
// teneur en eau séparés, pas regroupés par type hr_t/retrait/teneur_eau —
// demande explicite du 14/08/2026). `type` (hr_t/retrait/teneur_eau, ce
// dont le backend a besoin pour choisir la mesure InfluxDB et les tags) et
// `champ` (le champ InfluxDB précis, utilisé par GraphiqueSVG pour tracer
// la bonne courbe) sont dérivés automatiquement de la grandeur choisie et
// portés dans `valeur` — un champ en trop (`champ`) que le backend ignore
// silencieusement (Pydantic extra="ignore" par défaut) ne casse rien.
//
// Mur/couche en <input list> (autocomplete natif) plutôt qu'un menu
// déroulant strict : les vraies valeurs en base sont du texte libre, pas
// des noms canoniques (ex. "interface carreau et exterieur", pas
// "carreau_ext" — cf. logique_projet.md section 32, découverte du
// 12/08/2026) — /api/mesures/valeurs-tags les propose sans empêcher de
// saisir une valeur pas encore vue (ex. un nouveau capteur).
export default function SelecteurMesure({ valeur, onChange }) {
  const [combinaisons, setCombinaisons] = useState([]);

  useEffect(() => {
    api.mesuresValeursTags({ type: valeur.type }).then((r) => setCombinaisons(r.combinaisons)).catch(() => setCombinaisons([]));
  }, [valeur.type]);

  const definir = (champ, val) => onChange({ ...valeur, [champ]: val });

  const definirGrandeur = (grandeurValeur) => {
    const [type, champ] = grandeurValeur.split(":");
    onChange({ ...valeur, type, champ });
  };

  const grandeurActuelle = `${valeur.type}:${valeur.champ}`;

  const cleMur = valeur.type === "teneur_eau" ? "mur" : "nom_mur";
  const cleCouche = valeur.type === "teneur_eau" ? "couche" : "nom_couche";
  const murs = [...new Set(combinaisons.map((c) => c[cleMur]).filter(Boolean))];
  const couches = [...new Set(combinaisons.map((c) => c[cleCouche]).filter(Boolean))];

  return (
    <div className="selection-form">
      <div className="champ">
        <label>Grandeur</label>
        <select value={grandeurActuelle} onChange={(e) => definirGrandeur(e.target.value)}>
          {GRANDEURS_MESURABLES.map((g) => (
            <option key={g.valeur} value={g.valeur}>{g.label}</option>
          ))}
        </select>
      </div>
      <div className="champ">
        <label>Mur</label>
        <input list="murs-connus" value={valeur.mur || ""} onChange={(e) => definir("mur", e.target.value)} placeholder="ex. SOCMA 1" />
        <datalist id="murs-connus">{murs.map((m) => <option key={m} value={m} />)}</datalist>
      </div>
      <div className="champ">
        <label>Couche</label>
        <input list="couches-connues" value={valeur.couche || ""} onChange={(e) => definir("couche", e.target.value)} placeholder="ex. milieu isolant" />
        <datalist id="couches-connues">{couches.map((c) => <option key={c} value={c} />)}</datalist>
      </div>
      {valeur.type === "retrait" && (
        <div className="champ">
          <label>Canal</label>
          <input value={valeur.canal_nom || ""} onChange={(e) => definir("canal_nom", e.target.value)} placeholder="ex. HA1" />
        </div>
      )}
      <div className="champ">
        <label>Début</label>
        <input type="date" value={valeur.debut || ""} onChange={(e) => definir("debut", e.target.value)} />
      </div>
      <div className="champ">
        <label>Fin</label>
        <input type="date" value={valeur.fin || ""} onChange={(e) => definir("fin", e.target.value)} />
      </div>
    </div>
  );
}
