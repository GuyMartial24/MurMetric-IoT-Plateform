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
// Mur ET Couche en <select> strict (pas un <input list>/datalist) —
// abandonné le 14/08/2026 après deux bugs de suite avec le même
// mécanisme : un <input list> pré-rempli (mur par défaut "SOCMA 1", ou
// pire, un ancien défaut "couche" invalide "carreau_ext" qui n'existe même
// pas en base) filtre les suggestions de la <datalist> sur le texte déjà
// présent, masquant silencieusement les vraies valeurs (ex. "SOCMA 2"
// invisible, puis "toutes les couches" invisibles) sans que rien ne soit
// réellement manquant côté données — vérifié en direct les deux fois via
// /api/mesures/valeurs-tags. Un <select> ne peut pas reproduire cette
// classe de bug : il affiche toujours toutes les options, jamais filtrées
// sur la valeur courante. La casse parfois incohérente des couches en
// base (ex. "Milieu carreau" vs "milieu carreau" — cf. logique_projet.md
// section 32, découverte du 12/08/2026) n'est plus un problème : le menu
// affiche les valeurs telles qu'elles existent réellement, pas besoin de
// deviner l'orthographe exacte en tapant. Couche reste optionnelle
// ("— toutes —" en premier choix) — Mur ne l'est pas dans l'usage courant.
export default function SelecteurMesure({ valeur, onChange }) {
  const [combinaisons, setCombinaisons] = useState([]);

  useEffect(() => {
    api
      .mesuresValeursTags({ type: valeur.type })
      .then((r) => setCombinaisons(r.combinaisons))
      .catch(() => setCombinaisons([]));
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
  // canal_nom (clé du registre capteurs_retrait.json) ne change jamais, même
  // après un renommage de Mur/Couche — contrairement à ces derniers, toujours
  // fiable pour retrouver l'historique complet d'un canal (cf. discussion du
  // 18/08/2026, logique_projet.md section 34).
  const canaux = [...new Set(combinaisons.map((c) => c.canal_nom).filter(Boolean))];

  return (
    <div className="selection-form">
      <div className="champ">
        <label>Grandeur</label>
        <select value={grandeurActuelle} onChange={(e) => definirGrandeur(e.target.value)}>
          {GRANDEURS_MESURABLES.map((g) => (
            <option key={g.valeur} value={g.valeur}>
              {g.label}
            </option>
          ))}
        </select>
      </div>
      <div className="champ">
        <label>Mur</label>
        <select value={valeur.mur || ""} onChange={(e) => definir("mur", e.target.value)}>
          {murs.length === 0 && <option value={valeur.mur || ""}>{valeur.mur || "Chargement..."}</option>}
          {murs.map((m) => (
            <option key={m} value={m}>
              {m}
            </option>
          ))}
        </select>
      </div>
      <div className="champ">
        <label>Couche</label>
        <select value={valeur.couche || ""} onChange={(e) => definir("couche", e.target.value)}>
          <option value="">— toutes —</option>
          {couches.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
      </div>
      {valeur.type === "retrait" && (
        <div className="champ">
          <label>Canal</label>
          <select value={valeur.canal_nom || ""} onChange={(e) => definir("canal_nom", e.target.value)}>
            <option value="">— tous —</option>
            {canaux.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
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
