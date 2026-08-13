import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api.js";

// Nomogramme 3D — composition libre d'axes entre HR/T et retrait, y compris
// le TEMPS comme axe à part entière (demandes explicites du 13/08/2026) +
// options de vue façon POC (rotation auto, vues préréglées). Projection en
// perspective classique (rotation yaw/pitch puis division perspective) —
// même principe mathématique générique que le POC, réécrit proprement
// (cf. logique_projet.md section 32 : le POC lui-même, ~50 fonctions très
// couplées à son propre DOM, n'a pas été repris tel quel).
const AXES_GRANDEURS = [
  { valeur: "hr_t:temperature", label: "Température (°C)" },
  { valeur: "hr_t:humidite", label: "Humidité (%)" },
  { valeur: "hr_t:point_de_rosee", label: "Point de rosée (°C)" },
  { valeur: "retrait:valeur_filtree", label: "Retrait filtré" },
  { valeur: "retrait:valeur", label: "Retrait brut" },
];
const AXES_DISPONIBLES = [{ valeur: "temps", label: "Temps" }, ...AXES_GRANDEURS];
const CANAUX_RETRAIT = ["HA1", "HA2", "VA1", "VA2", "HB1", "HB2", "VB1", "VB2"];

const UNITES_TEMPS = {
  heure: { diviseur: 3_600_000, label: "heures" },
  jour: { diviseur: 86_400_000, label: "jours" },
  semaine: { diviseur: 7 * 86_400_000, label: "semaines" },
  mois: { diviseur: 30.44 * 86_400_000, label: "mois" },
  annee: { diviseur: 365.25 * 86_400_000, label: "années" },
};

const VUES_PREREGLEES = {
  face: { yaw: 0, pitch: 0 },
  dessus: { yaw: 0, pitch: 1.45 },
  profil: { yaw: 1.5708, pitch: 0 },
  isometrique: { yaw: 0.6, pitch: 0.35 },
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

const ROLES = ["x", "y", "z"];
const CLES_BACKEND = ["axe_x", "axe_y", "axe_z"];

export default function Nomogramme3D({ mur, couche }) {
  const [axeX, setAxeX] = useState("temps");
  const [axeY, setAxeY] = useState("hr_t:temperature");
  const [axeZ, setAxeZ] = useState("hr_t:humidite");
  const [canal, setCanal] = useState("HA1");
  const [uniteTemps, setUniteTemps] = useState("jour");
  const [points, setPoints] = useState([]);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);
  const [survol, setSurvol] = useState(null);

  const [yaw, setYaw] = useState(VUES_PREREGLEES.isometrique.yaw);
  const [pitch, setPitch] = useState(VUES_PREREGLEES.isometrique.pitch);
  const [zoom, setZoom] = useState(1);
  const [rotationAuto, setRotationAuto] = useState(false);
  const canvasRef = useRef(null);
  const glisseRef = useRef(null);

  const choixParRole = { x: axeX, y: axeY, z: axeZ };
  const necessiteCanal = ROLES.some((r) => choixParRole[r].startsWith("retrait"));
  const necessiteUniteTemps = ROLES.some((r) => choixParRole[r] === "temps");
  const construireParamAxe = (axe) => (axe.startsWith("retrait") ? `${axe}:${canal}` : axe);

  function libelleAxe(role) {
    if (choixParRole[role] === "temps") return `Temps (${UNITES_TEMPS[uniteTemps].label})`;
    return AXES_GRANDEURS.find((a) => a.valeur === choixParRole[role])?.label ?? choixParRole[role];
  }

  const charger = async () => {
    const rolesReels = ROLES.filter((r) => choixParRole[r] !== "temps");
    if (rolesReels.length === 0) {
      setErreur("Choisis au moins une grandeur réelle en plus du temps.");
      setPoints([]);
      return;
    }
    setEnCours(true);
    setErreur(null);
    try {
      const params = { mur, couche };
      rolesReels.forEach((role, i) => {
        params[CLES_BACKEND[i]] = construireParamAxe(choixParRole[role]);
      });
      Object.keys(params).forEach((k) => (params[k] == null || params[k] === "") && delete params[k]);

      const resultat = await api.croisementLibre(params);
      const bruts = resultat?.points ?? [];
      const tempsMs = bruts.map((p) => new Date(p.time).getTime());
      const tMinMs = tempsMs.length ? Math.min(...tempsMs) : 0;
      const diviseur = UNITES_TEMPS[uniteTemps].diviseur;

      const finaux = bruts.map((p, i) => {
        const valeurs = {};
        rolesReels.forEach((role, idx) => {
          valeurs[role] = p[["x", "y", "z"][idx]];
        });
        ROLES.forEach((role) => {
          if (choixParRole[role] === "temps") valeurs[role] = (tempsMs[i] - tMinMs) / diviseur;
        });
        return { time: p.time, x: valeurs.x, y: valeurs.y, z: valeurs.z };
      });
      setPoints(finaux.filter((p) => p.x != null && p.y != null && p.z != null));
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  useEffect(() => {
    charger();
  }, [mur, couche, axeX, axeY, axeZ, canal, uniteTemps]); // eslint-disable-line react-hooks/exhaustive-deps

  // Rotation automatique — façon POC (autorotate), désactivée dès que
  // l'utilisateur prend la main à la souris (cf. surSourisBas).
  useEffect(() => {
    if (!rotationAuto) return;
    let brut;
    const boucle = () => {
      setYaw((y) => y + 0.006);
      brut = requestAnimationFrame(boucle);
    };
    brut = requestAnimationFrame(boucle);
    return () => cancelAnimationFrame(brut);
  }, [rotationAuto]);

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

    ctx.fillStyle = "#a0a6b5";
    ctx.font = "11px system-ui";
    const origine = proj(-1, -1, -1);
    const boutX = proj(1, -1, -1), boutY = proj(-1, 1, -1), boutZ = proj(-1, -1, 1);
    ctx.fillText(`${libelleAxe("x")}: ${bornes.xMin.toFixed(1)} → ${bornes.xMax.toFixed(1)}`, boutX.x, boutX.y);
    ctx.fillText(`${libelleAxe("y")}: ${bornes.yMin.toFixed(1)} → ${bornes.yMax.toFixed(1)}`, boutY.x, boutY.y);
    ctx.fillText(`${libelleAxe("z")}: ${bornes.zMin.toFixed(1)} → ${bornes.zMax.toFixed(1)}`, boutZ.x, boutZ.y);
    ctx.fillText("origine", origine.x - 20, origine.y + 14);

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
      ctx.fillRect(survol.x + 8, survol.y - 42, 170, 48);
      ctx.strokeStyle = "#7fd4ff";
      ctx.strokeRect(survol.x + 8, survol.y - 42, 170, 48);
      ctx.fillStyle = "#e6e6e6";
      ctx.fillText(`${libelleAxe("x")} = ${p.x.toFixed(2)}`, survol.x + 14, survol.y - 28);
      ctx.fillText(`${libelleAxe("y")} = ${p.y.toFixed(2)}`, survol.x + 14, survol.y - 16);
      ctx.fillText(`${libelleAxe("z")} = ${p.z.toFixed(2)}`, survol.x + 14, survol.y - 4);
    }
  }, [points, bornes, yaw, pitch, zoom, survol, axeX, axeY, axeZ, uniteTemps]); // eslint-disable-line react-hooks/exhaustive-deps

  const surSourisBas = (e) => {
    setRotationAuto(false);
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
  const appliquerVue = (nom) => {
    setRotationAuto(false);
    setYaw(VUES_PREREGLEES[nom].yaw);
    setPitch(VUES_PREREGLEES[nom].pitch);
  };

  return (
    <div>
      <div className="selection-form" style={{ marginBottom: "0.75rem" }}>
        <div className="champ">
          <label>Axe X</label>
          <select value={axeX} onChange={(e) => setAxeX(e.target.value)}>
            {AXES_DISPONIBLES.map((a) => <option key={a.valeur} value={a.valeur}>{a.label}</option>)}
          </select>
        </div>
        <div className="champ">
          <label>Axe Y</label>
          <select value={axeY} onChange={(e) => setAxeY(e.target.value)}>
            {AXES_DISPONIBLES.map((a) => <option key={a.valeur} value={a.valeur}>{a.label}</option>)}
          </select>
        </div>
        <div className="champ">
          <label>Axe Z</label>
          <select value={axeZ} onChange={(e) => setAxeZ(e.target.value)}>
            {AXES_DISPONIBLES.map((a) => <option key={a.valeur} value={a.valeur}>{a.label}</option>)}
          </select>
        </div>
        {necessiteUniteTemps && (
          <div className="champ">
            <label>Unité de temps</label>
            <select value={uniteTemps} onChange={(e) => setUniteTemps(e.target.value)}>
              <option value="heure">Heures</option>
              <option value="jour">Jours</option>
              <option value="semaine">Semaines</option>
              <option value="mois">Mois</option>
              <option value="annee">Années</option>
            </select>
          </div>
        )}
        {necessiteCanal && (
          <div className="champ">
            <label>Canal retrait</label>
            <select value={canal} onChange={(e) => setCanal(e.target.value)}>
              {CANAUX_RETRAIT.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
        )}
      </div>

      <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.5rem" }}>
        <button type="button" onClick={() => appliquerVue("face")}>Face</button>
        <button type="button" onClick={() => appliquerVue("dessus")}>Dessus</button>
        <button type="button" onClick={() => appliquerVue("profil")}>Profil</button>
        <button type="button" onClick={() => appliquerVue("isometrique")}>Isométrique</button>
        <button type="button" onClick={() => setRotationAuto((v) => !v)}>
          {rotationAuto ? "⏸ Arrêter la rotation" : "▶ Rotation automatique"}
        </button>
        <button type="button" onClick={() => setZoom(1)}>Réinitialiser le zoom</button>
        <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: "0.4rem", fontSize: "0.78rem", color: "#a0a6b5" }}>
          <span>ancien</span>
          <div style={{ width: "60px", height: "8px", borderRadius: "4px", background: "linear-gradient(90deg, hsl(220,80%,60%), hsl(0,80%,60%))" }} />
          <span>récent</span>
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
