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

export const UNITES_TEMPS = {
  heure: { diviseur: 3_600_000, label: "heures" },
  jour: { diviseur: 86_400_000, label: "jours" },
  semaine: { diviseur: 7 * 86_400_000, label: "semaines" },
  mois: { diviseur: 30.44 * 86_400_000, label: "mois" },
  annee: { diviseur: 365.25 * 86_400_000, label: "années" },
};

export function libelleGrandeur(valeur) {
  return AXES_GRANDEURS.find((a) => a.valeur === valeur)?.label ?? valeur;
}

export function construireParamAxe(axe, canal) {
  return axe.startsWith("retrait") ? `${axe}:${canal}` : axe;
}
