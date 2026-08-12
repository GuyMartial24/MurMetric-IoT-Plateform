import { useEffect, useState } from "react";
import { api } from "../api.js";

const TYPES = [
  { valeur: "hr_t", label: "Température / humidité (HR/T)" },
  { valeur: "retrait", label: "Retrait (DeweSoft)" },
  { valeur: "teneur_eau", label: "Teneur en eau" },
];

// Sélection partagée par la Vue d'ensemble et l'Assistant IA — même forme
// que le modèle `Selection` côté backend (app/routers/assistant.py).
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

  const cleMur = valeur.type === "teneur_eau" ? "mur" : "nom_mur";
  const cleCouche = valeur.type === "teneur_eau" ? "couche" : "nom_couche";
  const murs = [...new Set(combinaisons.map((c) => c[cleMur]).filter(Boolean))];
  const couches = [...new Set(combinaisons.map((c) => c[cleCouche]).filter(Boolean))];

  return (
    <div className="selection-form">
      <div className="champ">
        <label>Type de mesure</label>
        <select value={valeur.type} onChange={(e) => definir("type", e.target.value)}>
          {TYPES.map((t) => (
            <option key={t.valeur} value={t.valeur}>{t.label}</option>
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
