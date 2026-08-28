import { useState } from "react";

const VALEUR_AUTRE = "__autre__";

// Combo <select> (valeurs déjà connues) + bascule vers un <input> texte
// libre pour une valeur inédite — remplace un champ texte libre pur pour
// mur/couche (cf. incident de divergence de libellés du 28/08/2026,
// logique_projet.md) sans empêcher la saisie d'une paroi/couche réellement
// nouvelle. Pas de wrapper de mise en page (ni <label>, ni div.champ) :
// utilisé aussi bien dans un formulaire que dans une cellule de tableau,
// contextes trop différents pour un wrapper imposé ici.
export default function ChampSelectOuAutre({ valeur, options, onChange, placeholder, required }) {
  const [saisieLibre, setSaisieLibre] = useState(valeur !== "" && !options.includes(valeur));

  if (saisieLibre) {
    return (
      <span style={{ display: "inline-flex", gap: "0.3rem", alignItems: "center" }}>
        <input
          required={required}
          value={valeur}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
        />
        {options.length > 0 && (
          <button
            type="button"
            title="Choisir dans la liste existante"
            onClick={() => {
              setSaisieLibre(false);
              onChange("");
            }}
          >
            ☰
          </button>
        )}
      </span>
    );
  }

  return (
    <select
      required={required}
      value={options.includes(valeur) ? valeur : ""}
      onChange={(e) => {
        if (e.target.value === VALEUR_AUTRE) {
          setSaisieLibre(true);
          onChange("");
        } else {
          onChange(e.target.value);
        }
      }}
    >
      <option value="" disabled>
        — choisir —
      </option>
      {options.map((o) => (
        <option key={o} value={o}>
          {o}
        </option>
      ))}
      <option value={VALEUR_AUTRE}>+ Nouveau...</option>
    </select>
  );
}
