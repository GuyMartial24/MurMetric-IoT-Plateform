import { clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs) {
  return twMerge(clsx(inputs));
}

// Classes reprises du composant shadcn Input, pour habiller un <select> ou
// <input> natif (pas <Select>/Radix) là où le comportement natif doit rester
// exact — cellules de tableau denses (Capteurs.jsx, TeneurEau.jsx) ou champs
// mêlés à des <select> natifs pas encore migrés (Phase C, migration
// progressive page par page). Centralisé ici car réutilisé dans plusieurs
// fichiers (ChampSelectOuAutre.jsx, SelecteurMesure.jsx, et à venir).
export const classesChampNatif = cn(
  "h-8 w-full min-w-0 rounded-lg border border-input bg-transparent px-2.5 py-1 text-base outline-none transition-colors",
  "focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50",
  "disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50",
  "md:text-sm dark:bg-input/30",
);
