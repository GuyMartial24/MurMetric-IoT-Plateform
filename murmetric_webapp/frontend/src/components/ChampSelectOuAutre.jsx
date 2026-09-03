import { useState } from "react";
import { List } from "lucide-react";
import { Button } from "./ui/button.jsx";
import { classesChampNatif } from "../lib/utils.js";

const VALEUR_AUTRE = "__autre__";

// <select> et <input> restent des balises natives (pas de <Select> Radix
// ici, cf. classesChampNatif) : ce composant est aussi utilisé dans des
// cellules de tableau denses (Capteurs.jsx, TeneurEau.jsx), où le
// listbox/popover de Radix Select serait un changement de comportement
// plus risqué que nécessaire pour un simple habillage visuel.

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
      <span className="inline-flex items-center gap-1.5">
        <input
          required={required}
          value={valeur}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          className={classesChampNatif}
        />
        {options.length > 0 && (
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            title="Choisir dans la liste existante"
            onClick={() => {
              setSaisieLibre(false);
              onChange("");
            }}
          >
            <List />
          </Button>
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
      className={classesChampNatif}
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
