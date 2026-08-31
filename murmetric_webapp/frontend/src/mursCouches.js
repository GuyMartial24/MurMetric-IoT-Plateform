import { useEffect, useMemo, useState } from "react";
import { api } from "./api.js";

// Clé mur/couche selon le type de mesure — teneur_eau utilise mur/couche
// SANS le préfixe nom_ (cf. teneur_eau.py, _construire_ligne), contrairement
// à hr_t/retrait (nom_mur/nom_couche).
const CLES_PAR_TYPE = {
  hr_t: { mur: "nom_mur", couche: "nom_couche" },
  retrait: { mur: "nom_mur", couche: "nom_couche" },
  teneur_eau: { mur: "mur", couche: "couche" },
};

// Union des murs/couches déjà utilisés dans les 3 types de mesures —
// remplace la saisie texte libre dans les formulaires d'édition (TeneurEau,
// Capteurs) pour empêcher deux systèmes de diverger sur le nom d'une même
// paroi/couche (cf. incident de désynchronisation corrigé le 28/08/2026,
// logique_projet.md). Réutilise /api/mesures/valeurs-tags (déjà utilisé en
// lecture seule par SelecteurMesure.jsx) plutôt qu'un nouvel endpoint.
export async function chargerMursCouchesConnus() {
  const types = Object.keys(CLES_PAR_TYPE);
  const resultats = await Promise.all(
    types.map((type) => api.mesuresValeursTags({ type }).catch(() => ({ combinaisons: [] }))),
  );
  const murs = new Set();
  const couches = new Set();
  resultats.forEach((r, i) => {
    const { mur: cleMur, couche: cleCouche } = CLES_PAR_TYPE[types[i]];
    (r.combinaisons || []).forEach((c) => {
      if (c[cleMur]) murs.add(c[cleMur]);
      if (c[cleCouche]) couches.add(c[cleCouche]);
    });
  });
  return {
    murs: [...murs].sort((a, b) => a.localeCompare(b)),
    couches: [...couches].sort((a, b) => a.localeCompare(b)),
  };
}

export function useMursCouchesConnus() {
  const [options, setOptions] = useState({ murs: [], couches: [] });
  useEffect(() => {
    chargerMursCouchesConnus().then(setOptions);
  }, []);
  return options;
}

// Couches d'UN SEUL type de mesure, filtrées par mur (31/08/2026, demande
// explicite) — contrairement à useMursCouchesConnus() ci-dessus (union
// globale des 3 types, jamais filtrée par mur), utilisé là où mélanger des
// couches d'un autre type n'a pas de sens (ex. proposer les couches HR/T
// dans un formulaire teneur en eau) ou induit en erreur (ex. proposer la
// couche d'un autre mur que celui déjà choisi). `type` accepté tel quel par
// CLES_PAR_TYPE (hr_t/retrait/teneur_eau).
export function useCouchesParMur(type, mur) {
  const [combinaisons, setCombinaisons] = useState(null);
  useEffect(() => {
    api
      .mesuresValeursTags({ type })
      .then((r) => setCombinaisons(r?.combinaisons ?? []))
      .catch(() => setCombinaisons([]));
  }, [type]);

  const { mur: cleMur, couche: cleCouche } = CLES_PAR_TYPE[type];
  return useMemo(() => {
    if (!combinaisons) return [];
    return [
      ...new Set(
        combinaisons
          .filter((c) => !mur || c[cleMur] === mur)
          .map((c) => c[cleCouche])
          .filter(Boolean),
      ),
    ].sort((a, b) => a.localeCompare(b));
  }, [combinaisons, mur, cleMur, cleCouche]);
}
