// Export de données tabulaires en CSV/Excel (demande explicite du
// 14/08/2026) — CSV en JS pur (aucune dépendance, format universel) ;
// Excel via SheetJS (xlsx), installé depuis cdn.sheetjs.com plutôt que le
// paquet npm officiel : la copie npm est figée sur une version portant des
// failles connues (pollution de prototype, ReDoS) jamais corrigées côté
// registre, SheetJS distribue les correctifs uniquement via son propre CDN.
// Import dynamique (~330 ko) : ne doit pas alourdir le chargement initial
// de l'appli pour une action que la plupart des utilisateurs ne feront
// jamais dans une session donnée.

function colonnes(lignes) {
  const cles = new Set();
  for (const ligne of lignes) {
    for (const cle of Object.keys(ligne)) cles.add(cle);
  }
  return [...cles];
}

function echapperCSV(valeur) {
  const s = valeur == null ? "" : String(valeur);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function telecharger(blob, nomFichier) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = nomFichier;
  a.click();
  URL.revokeObjectURL(url);
}

const BOM_UTF8 = String.fromCharCode(0xfeff);

export function telechargerCSV(lignes, nomFichier) {
  const cles = colonnes(lignes);
  const entete = cles.map(echapperCSV).join(",");
  const corps = lignes.map((ligne) => cles.map((c) => echapperCSV(ligne[c])).join(","));
  // Sans le BOM, Excel (Windows) mal-interprète les accents à l'ouverture
  // d'un CSV — sans effet ailleurs (navigateurs, LibreOffice).
  const contenu = BOM_UTF8 + [entete, ...corps].join("\r\n");
  telecharger(new Blob([contenu], { type: "text/csv;charset=utf-8" }), nomFichier);
}

export async function telechargerExcel(lignes, nomFichier) {
  const XLSX = await import("xlsx");
  const feuille = XLSX.utils.json_to_sheet(lignes, { header: colonnes(lignes) });
  const classeur = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(classeur, feuille, "Données");
  XLSX.writeFile(classeur, nomFichier);
}
