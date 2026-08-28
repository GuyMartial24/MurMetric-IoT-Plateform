import { useEffect, useMemo, useState } from "react";
import { api } from "./api.js";

// Découverte dynamique des canaux retrait, filtrée par mur (28/08/2026,
// demande explicite) — remplace la liste figée à 8 noms (CANAUX_RETRAIT,
// nomogrammeAxes.js, toujours utilisée par Export.jsx/FiltreHampel.jsx qui
// n'ont pas cette notion de "Mur" déjà sélectionné) pour le sélecteur
// Canal retrait du nomogramme :
// - Filtrée sur le "Mur" déjà choisi plus haut dans la page (celui utilisé
//   pour HR/T) — avant ce correctif, le canal était indépendant de ce mur,
//   et une combinaison incompatible (ex. mur SOCMA 1 + canal HB1, qui
//   appartient à SOCMA 2) renvoyait silencieusement 0 point sans message
//   clair (mur ET canal_nom filtrés simultanément côté Flux).
// - Découverte depuis le registre (pas figée) : un canal futur (ex. HA3)
//   apparaît automatiquement, individuellement ET dans son groupe de
//   moyenne, sans modification de code.
//
// Convention de nommage (vérifiée en direct dans le registre le
// 28/08/2026, PAS dans logique_projet.md qui s'est révélé ambigu sur ce
// point précis) : `[H|V][lettre mur][numéro position]`, ex. "HA1" =
// Horizontal, mur A (SOCMA 1), position 1 (bas). Le numéro distingue la
// POSITION sur un même mur/orientation, jamais le mur lui-même — donc
// moyenner deux canaux du même groupe (même lettre H/V + même lettre mur)
// ne mélange jamais deux murs ni deux orientations, contrairement à "Tous"
// qui mélange tout.
const RE_CANAL = /^([HV])([A-Z])(\d+)$/;

export function grouperPourMoyenne(canaux) {
  const groupes = {};
  canaux.forEach((c) => {
    const m = RE_CANAL.exec(c);
    if (!m) return;
    const cle = m[1] + m[2]; // ex. "HA"
    (groupes[cle] ??= []).push(c);
  });
  return Object.values(groupes)
    .filter((membres) => membres.length >= 2)
    .map((membres) => {
      const tries = [...membres].sort();
      return { valeur: tries.join("+"), label: `Moy(${tries.join(",")})` };
    });
}

export function useCanauxRetrait(mur) {
  const [registre, setRegistre] = useState(null);
  useEffect(() => {
    api
      .capteursRetrait()
      .then(setRegistre)
      .catch(() => setRegistre({}));
  }, []);

  return useMemo(() => {
    if (!registre) return { canaux: [], moyennes: [] };
    const canaux = Object.entries(registre)
      .filter(([cle, infos]) => !cle.startsWith("_") && (!mur || infos.nom_mur === mur))
      .map(([cle]) => cle)
      .sort();
    return { canaux, moyennes: grouperPourMoyenne(canaux) };
  }, [registre, mur]);
}
