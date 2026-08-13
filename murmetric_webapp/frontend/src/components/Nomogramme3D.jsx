import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api.js";

// Nomogramme 3D — suite du portage scopé du POC (Nomogramme.jsx fait le
// croisement 2D ; ici la vraie rotation 3D demandée explicitement le
// 13/08/2026). Projection en perspective classique (rotation yaw/pitch puis
// division perspective) — même principe mathématique générique que le POC,
// réécrit proprement pour ce composant plutôt que porté ligne à ligne (cf.
// logique_projet.md section 32 : le POC lui-même n'a pas été repris tel
// quel, ~50 fonctions très couplées à son propre DOM).
const CHAMPS_PAR_TYPE = {
  hr_t: [
    { valeur: "temperature", label: "Température (°C)" },
    { valeur: "humidite", label: "Humidité (%)" },
    { valeur: "point_de_rosee", label: "Point de rosée (°C)" },
  ],
  retrait: [
    { valeur: "valeur", label: "Valeur brute" },
    { valeur: "valeur_filtree", label: "Valeur filtrée (Hampel)" },
  ],
};

function projeter(nx, ny, nz, yaw, pitch, zoom, w, h) {
  const cy = Math.cos(yaw), sy = Math.sin(yaw);
  const cp = Math.cos(pitch), sp = Math.sin(pitch);
  const x1 = nx * cy + nz * sy;
  const z1 = -nx * sy + nz * cy;
  const y1 = ny * cp - z1 * sp;
  const z2 = ny * sp + z1 * cp;
  const perspective = 3.4;
  const echelle = (perspective / (perspective + z2)) * Math.min(w, h) * 0.28 * zoom;
  return { x: w / 2 + x1 * echelle, y: h / 2 - y1 * echelle, profondeur: z2 };
}

const SOMMETS_CUBE = [];
for (let cx = -1; cx <= 1; cx += 2)
  for (let cy = -1; cy <= 1; cy += 2)
    for (let cz = -1; cz <= 1; cz += 2)
      SOMMETS_CUBE.push([cx, cy, cz]);
const ARETES_CUBE = [
  [0, 1], [0, 2], [0, 4], [3, 1], [3, 2], [3, 7],
  [5, 1], [5, 4], [5, 7], [6, 2], [6, 4], [6, 7],
];

export default function Nomogramme3D({ type, mur, couche, position }) {
  const champs = CHAMPS_PAR_TYPE[type] ?? [];
  const [champX, setChampX] = useState(champs[0]?.valeur);
  const [champY, setChampY] = useState(champs[1]?.valeur);
  const [champZ, setChampZ] = useState(champs[2]?.valeur ?? champs[0]?.valeur);
  const [points, setPoints] = useState([]);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);
  const [survol, setSurvol] = useState(null);

  const [yaw, setYaw] = useState(0.6);
  const [pitch, setPitch] = useState(0.35);
  const [zoom, setZoom] = useState(1);
  const canvasRef = useRef(null);
  const glisseRef = useRef(null);

  useEffect(() => {
    if (champs.length < 2) return;
    if (!champs.some((c) => c.valeur === champX)) setChampX(champs[0].valeur);
    if (!champs.some((c) => c.valeur === champY)) setChampY(champs[1]?.valeur ?? champs[0].valeur);
    if (!champs.some((c) => c.valeur === champZ)) setChampZ(champs[2]?.valeur ?? champs[0].valeur);
  }, [type]); // eslint-disable-line react-hooks/exhaustive-deps

  const charger = async () => {
    if (!champX || !champY || !champZ) return;
    setEnCours(true);
    setErreur(null);
    try {
      const params = Object.fromEntries(
        Object.entries({ type, mur, couche, position, champ_x: champX, champ_y: champY, champ_z: champZ }).filter(([, v]) => v),
      );
      const resultat = await api.croisement(params);
      setPoints((resultat?.points ?? []).filter((p) => p.x != null && p.y != null && p.z != null));
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  useEffect(() => {
    charger();
  }, [type, mur, couche, position, champX, champY, champZ]); // eslint-disable-line react-hooks/exhaustive-deps

  const bornes = useMemo(() => {
    if (points.length === 0) return null;
    const xs = points.map((p) => p.x), ys = points.map((p) => p.y), zs = points.map((p) => p.z);
    return {
      xMin: Math.min(...xs), xMax: Math.max(...xs),
      yMin: Math.min(...ys), yMax: Math.max(...ys),
      zMin: Math.min(...zs), zMax: Math.max(...zs),
    };
  }, [points]);

  const normaliser = (p, b) => [
    b.xMax > b.xMin ? ((p.x - b.xMin) / (b.xMax - b.xMin)) * 2 - 1 : 0,
    b.yMax > b.yMin ? ((p.y - b.yMin) / (b.yMax - b.yMin)) * 2 - 1 : 0,
    b.zMax > b.zMin ? ((p.z - b.zMin) / (b.zMax - b.zMin)) * 2 - 1 : 0,
  ];

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !bornes) return;
    const w = canvas.clientWidth, h = canvas.clientHeight;
    canvas.width = w * devicePixelRatio;
    canvas.height = h * devicePixelRatio;
    const ctx = canvas.getContext("2d");
    ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
    ctx.clearRect(0, 0, w, h);

    const proj = (nx, ny, nz) => projeter(nx, ny, nz, yaw, pitch, zoom, w, h);

    // Arête du cube-cadre (repère spatial).
    ctx.strokeStyle = "#2a2e3a";
    ctx.lineWidth = 1;
    for (const [a, b] of ARETES_CUBE) {
      const pa = proj(...SOMMETS_CUBE[a]);
      const pb = proj(...SOMMETS_CUBE[b]);
      ctx.beginPath();
      ctx.moveTo(pa.x, pa.y);
      ctx.lineTo(pb.x, pb.y);
      ctx.stroke();
    }

    // Étiquettes des axes (min/max) depuis le coin (-1,-1,-1).
    ctx.fillStyle = "#a0a6b5";
    ctx.font = "11px system-ui";
    const origine = proj(-1, -1, -1);
    const boutX = proj(1, -1, -1), boutY = proj(-1, 1, -1), boutZ = proj(-1, -1, 1);
    ctx.fillText(`${champX}: ${bornes.xMin.toFixed(1)} → ${bornes.xMax.toFixed(1)}`, boutX.x, boutX.y);
    ctx.fillText(`${champY}: ${bornes.yMin.toFixed(1)} → ${bornes.yMax.toFixed(1)}`, boutY.x, boutY.y);
    ctx.fillText(`${champZ}: ${bornes.zMin.toFixed(1)} → ${bornes.zMax.toFixed(1)}`, boutZ.x, boutZ.y);
    ctx.fillText("origine", origine.x - 20, origine.y + 14);

    // Points, triés par profondeur (peintre) pour une occlusion correcte,
    // couleur = position temporelle (bleu = ancien, rouge = récent).
    const temps = points.map((p) => new Date(p.time).getTime());
    const [tMin, tMax] = [Math.min(...temps), Math.max(...temps)];
    const projetes = points.map((p, i) => {
      const [nx, ny, nz] = normaliser(p, bornes);
      return { ...proj(nx, ny, nz), point: p, frac: tMax > tMin ? (temps[i] - tMin) / (tMax - tMin) : 0 };
    });
    projetes.sort((a, b) => a.profondeur - b.profondeur);
    for (const pp of projetes) {
      const hue = 220 - pp.frac * 220;
      const estSurvole = survol && survol.point === pp.point;
      ctx.fillStyle = estSurvole ? "#ffffff" : `hsl(${hue}, 80%, 60%)`;
      ctx.beginPath();
      ctx.arc(pp.x, pp.y, estSurvole ? 5 : 3, 0, 2 * Math.PI);
      ctx.fill();
    }

    if (survol) {
      const p = survol.point;
      ctx.fillStyle = "#0f1117";
      ctx.fillRect(survol.x + 8, survol.y - 42, 150, 48);
      ctx.strokeStyle = "#7fd4ff";
      ctx.strokeRect(survol.x + 8, survol.y - 42, 150, 48);
      ctx.fillStyle = "#e6e6e6";
      ctx.fillText(`${champX} = ${p.x.toFixed(2)}`, survol.x + 14, survol.y - 28);
      ctx.fillText(`${champY} = ${p.y.toFixed(2)}`, survol.x + 14, survol.y - 16);
      ctx.fillText(`${champZ} = ${p.z.toFixed(2)}`, survol.x + 14, survol.y - 4);
    }
  }, [points, bornes, yaw, pitch, zoom, survol, champX, champY, champZ]);

  const surSourisBas = (e) => {
    glisseRef.current = { x: e.clientX, y: e.clientY, yaw, pitch };
  };
  const surSourisDeplace = (e) => {
    if (glisseRef.current) {
      const dx = e.clientX - glisseRef.current.x;
      const dy = e.clientY - glisseRef.current.y;
      setYaw(glisseRef.current.yaw + dx * 0.01);
      setPitch(Math.max(-1.4, Math.min(1.4, glisseRef.current.pitch - dy * 0.01)));
      return;
    }
    if (points.length === 0 || !bornes) return;
    const canvas = canvasRef.current;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left, my = e.clientY - rect.top;
    const w = canvas.clientWidth, h = canvas.clientHeight;
    let plusProche = null, distanceMin = Infinity, meilleurProj = null;
    for (const p of points) {
      const [nx, ny, nz] = normaliser(p, bornes);
      const pp = projeter(nx, ny, nz, yaw, pitch, zoom, w, h);
      const d = (pp.x - mx) ** 2 + (pp.y - my) ** 2;
      if (d < distanceMin) {
        distanceMin = d;
        plusProche = p;
        meilleurProj = pp;
      }
    }
    setSurvol(distanceMin < 400 ? { point: plusProche, x: meilleurProj.x, y: meilleurProj.y } : null);
  };
  const surSourisHaut = () => {
    glisseRef.current = null;
  };
  const surMolette = (e) => {
    e.preventDefault();
    setZoom((z) => Math.max(0.4, Math.min(3, z - e.deltaY * 0.001)));
  };

  if (champs.length < 3) {
    return <p style={{ color: "#a0a6b5" }}>Pas assez de grandeurs disponibles pour un nomogramme 3D sur ce type de mesure.</p>;
  }

  return (
    <div>
      <div className="selection-form" style={{ marginBottom: "0.75rem" }}>
        <div className="champ">
          <label>Axe X</label>
          <select value={champX} onChange={(e) => setChampX(e.target.value)}>
            {champs.map((c) => <option key={c.valeur} value={c.valeur}>{c.label}</option>)}
          </select>
        </div>
        <div className="champ">
          <label>Axe Y</label>
          <select value={champY} onChange={(e) => setChampY(e.target.value)}>
            {champs.map((c) => <option key={c.valeur} value={c.valeur}>{c.label}</option>)}
          </select>
        </div>
        <div className="champ">
          <label>Axe Z</label>
          <select value={champZ} onChange={(e) => setChampZ(e.target.value)}>
            {champs.map((c) => <option key={c.valeur} value={c.valeur}>{c.label}</option>)}
          </select>
        </div>
        <div className="champ" style={{ justifyContent: "flex-end" }}>
          <label>&nbsp;</label>
          <button type="button" onClick={() => { setYaw(0.6); setPitch(0.35); setZoom(1); }}>
            Réinitialiser la vue
          </button>
        </div>
      </div>
      <p style={{ color: "#a0a6b5", fontSize: "0.8rem", margin: "0 0 0.5rem" }}>
        Glisser pour tourner · molette pour zoomer · survoler un point pour lire ses 3 valeurs.
      </p>
      {erreur && <p className="erreur">{erreur}</p>}
      {enCours && <p style={{ color: "#a0a6b5" }}>Chargement...</p>}
      {!enCours && points.length === 0 && !erreur && <p style={{ color: "#a0a6b5" }}>Aucun point croisé pour cette sélection.</p>}
      <canvas
        ref={canvasRef}
        style={{ width: "100%", height: "480px", cursor: glisseRef.current ? "grabbing" : "grab" }}
        onMouseDown={surSourisBas}
        onMouseMove={surSourisDeplace}
        onMouseUp={surSourisHaut}
        onMouseLeave={surSourisHaut}
        onWheel={surMolette}
      />
    </div>
  );
}
