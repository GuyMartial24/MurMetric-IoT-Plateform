import { forwardRef, useImperativeHandle, useRef } from "react";
import BoutonsExport from "./BoutonsExport.jsx";

// Tracé minimal en SVG pur (pas de dépendance de charting) — représentation
// provisoire en attendant le portage du moteur Canvas de l'abaque POC
// (data_reel_compile/abaque-3d-hygrothermique.html, cf. logique_projet.md
// section 32) en composant React.
//
// forwardRef expose l'élément <svg> au parent (cf. Assistant.jsx, capture
// de l'image pour l'analyse vision) — indépendant de BoutonsExport, qui
// garde son propre accès interne au même svgRef.
const GraphiqueSVG = forwardRef(function GraphiqueSVG({ points, champ }, ref) {
  const svgRef = useRef(null);
  useImperativeHandle(ref, () => svgRef.current);
  const valeurs = points.filter((p) => p.field === champ);
  if (valeurs.length === 0) {
    return <p>Aucun point pour ce champ sur la période sélectionnée.</p>;
  }

  const largeur = 800;
  const hauteur = 260;
  const marge = 30;

  const temps = valeurs.map((p) => new Date(p.time).getTime());
  const vals = valeurs.map((p) => p.value);
  const [tMin, tMax] = [Math.min(...temps), Math.max(...temps)];
  const [vMin, vMax] = [Math.min(...vals), Math.max(...vals)];

  const x = (t) => marge + ((t - tMin) / (tMax - tMin || 1)) * (largeur - 2 * marge);
  const y = (v) => hauteur - marge - ((v - vMin) / (vMax - vMin || 1)) * (hauteur - 2 * marge);

  const chemin = valeurs
    .map((p, i) => `${i === 0 ? "M" : "L"}${x(new Date(p.time).getTime())},${y(p.value)}`)
    .join(" ");

  return (
    <div>
      <svg ref={svgRef} viewBox={`0 0 ${largeur} ${hauteur}`} width="100%" height={hauteur}>
        <path d={chemin} fill="none" stroke="#7fd4ff" strokeWidth="1.5" />
        <text x={marge} y={16} fill="#a0a6b5" fontSize="12">
          {champ} — max {vMax.toFixed(2)}
        </text>
        <text x={marge} y={hauteur - 8} fill="#a0a6b5" fontSize="12">
          min {vMin.toFixed(2)} — {valeurs.length} points
        </text>
      </svg>
      <BoutonsExport obtenirElement={() => svgRef.current} type="svg" nomFichier={`courbe-${champ}`} />
    </div>
  );
});

export default GraphiqueSVG;
