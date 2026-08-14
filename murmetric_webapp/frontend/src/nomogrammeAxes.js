// Catalogue d'axes partagé entre le nomogramme 2D (Nomogramme.jsx) et 3D
// (Nomogramme3D.jsx) — identique dans les deux, demande explicite du
// 13/08/2026. "temps" est un axe virtuel : jamais envoyé au backend,
// calculé côté client à partir de l'horodatage de chaque point (cf.
// logique_projet.md section 32, "Axe Temps dans le nomogramme 3D").
export const AXES_GRANDEURS = [
  { valeur: "hr_t:temperature", label: "Température (°C)" },
  { valeur: "hr_t:humidite", label: "Humidité (%)" },
  { valeur: "hr_t:point_de_rosee", label: "Point de rosée (°C)" },
  { valeur: "retrait:valeur_filtree", label: "Retrait filtré" },
  { valeur: "retrait:valeur", label: "Retrait brut" },
];
export const AXES_DISPONIBLES = [{ valeur: "temps", label: "Temps" }, ...AXES_GRANDEURS];
export const CANAUX_RETRAIT = ["HA1", "HA2", "VA1", "VA2", "HB1", "HB2", "VB1", "VB2"];

// Catalogue complet des grandeurs mesurables — AXES_GRANDEURS + teneur en
// eau, pour le sélecteur "Grandeur" de Vue d'ensemble/Assistant IA (demande
// explicite du 14/08/2026 : chaque grandeur séparée plutôt que regroupée
// par type hr_t/retrait). Teneur en eau volontairement EXCLUE du catalogue
// nomogramme ci-dessus : données éparses saisies manuellement, pas de sens
// à la croiser avec des séries denses hr_t/retrait — mais elle doit rester
// choisissable comme grandeur à charger/analyser seule.
export const GRANDEURS_MESURABLES = [
  ...AXES_GRANDEURS,
  { valeur: "teneur_eau:teneur_eau_pourcent", label: "Teneur en eau (%)" },
];

// Type de tracé (façon POC : nuage de points, ou trait reliant les points
// dans l'ordre chronologique — utile pour suivre une trajectoire dans
// l'espace des grandeurs plutôt qu'un simple nuage) — demande explicite
// du 13/08/2026, partagé entre 2D et 3D.
export const TYPES_TRACE = [
  { valeur: "nuage", label: "Nuage de points" },
  { valeur: "trait", label: "Trait fin" },
  { valeur: "nuage_trait", label: "Nuage + trait" },
];

export const UNITES_TEMPS = {
  heure: { diviseur: 3_600_000, label: "heures" },
  jour: { diviseur: 86_400_000, label: "jours" },
  semaine: { diviseur: 7 * 86_400_000, label: "semaines" },
  mois: { diviseur: 30.44 * 86_400_000, label: "mois" },
  annee: { diviseur: 365.25 * 86_400_000, label: "années" },
};

// GRANDEURS_MESURABLES (superset incluant teneur en eau) plutôt que
// AXES_GRANDEURS : cette fonction sert aussi hors nomogramme (en-tête de
// courbe dans Vue d'ensemble/Assistant IA, demande du 14/08/2026) où la
// teneur en eau est une grandeur choisissable.
export function libelleGrandeur(valeur) {
  return GRANDEURS_MESURABLES.find((a) => a.valeur === valeur)?.label ?? valeur;
}

export function construireParamAxe(axe, canal) {
  return axe.startsWith("retrait") ? `${axe}:${canal}` : axe;
}

// Lecture de valeur par projection façon POC (section 24) : pas seulement
// survoler un point existant, mais choisir une valeur cible sur UN axe et
// trouver où la trajectoire (points dans l'ordre chronologique, traités
// comme des segments reliés même en mode "nuage") la croise, en interpolant
// linéairement les autres axes à cet endroit précis — x→y, y→x (et les deux
// autres axes en 3D). Une trajectoire qui va et vient peut croiser une
// valeur plusieurs fois : tous les croisements sont renvoyés, pas
// seulement le premier.
export function trouverCroisements(points, axeCible, valeurCible, autresAxes) {
  const resultats = [];
  for (let i = 0; i < points.length - 1; i++) {
    const a = points[i][axeCible];
    const b = points[i + 1][axeCible];
    if (a == null || b == null) continue;
    if (!((a <= valeurCible && valeurCible <= b) || (b <= valeurCible && valeurCible <= a))) continue;
    const t = a === b ? 0 : (valeurCible - a) / (b - a);
    const valeurs = { [axeCible]: valeurCible };
    autresAxes.forEach((axe) => {
      valeurs[axe] = points[i][axe] + t * (points[i + 1][axe] - points[i][axe]);
    });
    resultats.push(valeurs);
  }
  return resultats;
}
