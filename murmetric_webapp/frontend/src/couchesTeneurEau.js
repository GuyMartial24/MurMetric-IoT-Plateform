import { useEffect, useMemo, useState } from "react";
import { api } from "./api.js";

// Découverte dynamique des couches de teneur en eau pour un mur donné
// (30/08/2026, demande explicite) — alimente le sélecteur à choix multiple
// du panneau "retrait en fonction du temps" du nomogramme (axe teneur en
// eau, moyenne des couches cochées). Pas de convention de nommage
// exploitable ici, contrairement aux canaux retrait (cf. canauxRetrait.js) :
// 3 couches actuellement ("interface carreau et exterieur", "interface
// carreau isolant", "milieu isolant"), sans distinction binaire intérieur/
// extérieur — l'utilisateur choisit librement lesquelles moyenner plutôt
// qu'un regroupement imposé par le code.
export function useCouchesTeneurEau(mur) {
  const [combinaisons, setCombinaisons] = useState(null);
  useEffect(() => {
    api
      .mesuresValeursTags({ type: "teneur_eau" })
      .then((r) => setCombinaisons(r?.combinaisons ?? []))
      .catch(() => setCombinaisons([]));
  }, []);

  return useMemo(() => {
    if (!combinaisons) return [];
    return [...new Set(combinaisons.filter((c) => !mur || c.mur === mur).map((c) => c.couche))].sort();
  }, [combinaisons, mur]);
}
