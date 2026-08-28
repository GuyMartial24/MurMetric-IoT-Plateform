// Graduations d'un axe de valeur numérique (pas de graduation ronde), et de
// l'axe temps (alignée sur des débuts de période civile jour/semaine/mois/
// année) — déplacées ici depuis Nomogramme.jsx le 28/08/2026 lors de
// l'extension du panneau "évolution dans le temps" au nomogramme 3D,
// partagées entre les deux composants plutôt que dupliquées.
export function graduations(min, max, cible = 5) {
  if (min === max) return [min];
  const brut = (max - min) / cible;
  const magnitude = 10 ** Math.floor(Math.log10(brut));
  const normalise = brut / magnitude;
  const pas = (normalise < 1.5 ? 1 : normalise < 3 ? 2 : normalise < 7 ? 5 : 10) * magnitude;
  const debut = Math.ceil(min / pas) * pas;
  const valeurs = [];
  for (let v = debut; v <= max + pas * 1e-9; v += pas) valeurs.push(Number(v.toFixed(10)));
  return valeurs;
}

const RESOLUTIONS_TEMPS_MAX_TICKS = 15;

function debutPeriode(ms, resolution) {
  const d = new Date(ms);
  d.setHours(0, 0, 0, 0);
  if (resolution === "semaine") {
    const jour = d.getDay();
    d.setDate(d.getDate() - (jour === 0 ? 6 : jour - 1)); // lundi = début de semaine
  } else if (resolution === "mois") {
    d.setDate(1);
  } else if (resolution === "annee") {
    d.setMonth(0, 1);
  }
  return d;
}

function avancerPeriode(date, resolution) {
  const d = new Date(date);
  if (resolution === "jour") d.setDate(d.getDate() + 1);
  else if (resolution === "semaine") d.setDate(d.getDate() + 7);
  else if (resolution === "mois") d.setMonth(d.getMonth() + 1);
  else d.setFullYear(d.getFullYear() + 1);
  return d;
}

function formatterPeriode(date, resolution) {
  if (resolution === "annee") return date.getFullYear().toString();
  if (resolution === "mois") return date.toLocaleDateString("fr-FR", { month: "short", year: "2-digit" });
  return date.toLocaleDateString("fr-FR", { day: "2-digit", month: "2-digit" });
}

export function graduationsTemps(tMin, tMax, resolution) {
  const ticks = [];
  let curseur = debutPeriode(tMin, resolution);
  while (curseur.getTime() <= tMax) {
    if (curseur.getTime() >= tMin) ticks.push({ t: curseur.getTime(), label: formatterPeriode(curseur, resolution) });
    curseur = avancerPeriode(curseur, resolution);
  }
  if (ticks.length > RESOLUTIONS_TEMPS_MAX_TICKS) {
    const pas = Math.ceil(ticks.length / RESOLUTIONS_TEMPS_MAX_TICKS);
    return ticks.filter((_, i) => i % pas === 0);
  }
  return ticks;
}

// Catalogue d'axes partagé entre le nomogramme 2D (Nomogramme.jsx) et 3D
// (Nomogramme3D.jsx) — identique dans les deux, demande explicite du
// 13/08/2026. "temps" est un axe virtuel : jamais envoyé au backend,
// calculé côté client à partir de l'horodatage de chaque point (cf.
// logique_projet.md section 32, "Axe Temps dans le nomogramme 3D").
// Teneur en eau incluse malgré des données éparses/saisies manuellement
// (demande explicite utilisateur du 27/08/2026, malgré la mise en garde :
// le croisement/l'interpolation n'a de sens que si on choisit "Nuage de
// points" plutôt qu'un tracé relié — cf. croisement_libre côté backend,
// mesures.py, dont l'agrégation par fenêtre gère nativement l'éparsité
// sans produire de points fictifs entre deux relevés terrain).
export const AXES_GRANDEURS = [
  { valeur: "hr_t:temperature", label: "Température (°C)" },
  { valeur: "hr_t:humidite", label: "Humidité (%)" },
  { valeur: "hr_t:point_de_rosee", label: "Point de rosée (°C)" },
  { valeur: "retrait:valeur_filtree", label: "Retrait filtré" },
  { valeur: "retrait:valeur", label: "Retrait brut" },
  { valeur: "teneur_eau:teneur_eau_pourcent", label: "Teneur en eau (%)" },
];
export const AXES_DISPONIBLES = [{ valeur: "temps", label: "Temps" }, ...AXES_GRANDEURS];
export const CANAUX_RETRAIT = ["HA1", "HA2", "VA1", "VA2", "HB1", "HB2", "VB1", "VB2"];

// Couleur fixe par canal (28/08/2026) — "Tous" dans le sélecteur Canal
// retrait croise désormais RÉELLEMENT les 8 canaux (8 trajectoires
// distinctes superposées), remplace la moyenne initiale jugée peu
// pertinente physiquement (mélange horizontal/vertical, positions
// différentes — cf. discussion utilisateur du 28/08/2026). Palette fixe
// plutôt que le dégradé temporel habituel (ancien/récent), qui n'a plus de
// sens dès qu'on distingue des canaux plutôt qu'une seule trajectoire.
export const COULEURS_CANAUX_RETRAIT = {
  HA1: "#7fd4ff",
  HA2: "#7fff9e",
  VA1: "#ffb37f",
  VA2: "#ff7f9e",
  HB1: "#c47fff",
  HB2: "#ffe97f",
  VB1: "#7fffe0",
  VB2: "#ff9ff0",
};

// Catalogue complet des grandeurs mesurables, pour le sélecteur "Grandeur"
// de Vue d'ensemble/Assistant IA (demande explicite du 14/08/2026 : chaque
// grandeur séparée plutôt que regroupée par type hr_t/retrait). Alias de
// AXES_GRANDEURS depuis que la teneur en eau y est incluse (27/08/2026) —
// conservé comme export distinct pour ne pas toucher SelecteurMesure.jsx.
export const GRANDEURS_MESURABLES = AXES_GRANDEURS;

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
//
// `time` interpolé systématiquement (28/08/2026, demande explicite) — pas
// un axe demandable via autresAxes (jamais une grandeur), mais nécessaire
// pour replacer un croisement trouvé sur les panneaux temporels du
// nomogramme 2D : jusqu'ici, un point trouvé restait sans lien avec le
// moment réel où cette relation s'est produite dans les mesures.
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
    if (points[i].time != null && points[i + 1].time != null) {
      const tempsA = new Date(points[i].time).getTime();
      const tempsB = new Date(points[i + 1].time).getTime();
      valeurs.time = new Date(tempsA + t * (tempsB - tempsA)).toISOString();
    }
    resultats.push(valeurs);
  }
  return resultats;
}
