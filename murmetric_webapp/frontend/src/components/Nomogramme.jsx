import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api.js";

// Portage scopé du "nomogramme" de l'ancien POC (data_reel_compile/
// abaque-3d-hygrothermique.html, cf. logique_projet.md section 32) : le
// cœur unique (croiser deux grandeurs physiques l'une contre l'autre,
// pas contre le temps — ce que Grafana ne fait pas nativement) et la
// lecture de valeur par projection (lignes pointillées vers les axes au
// survol). Le reste des fonctionnalités du POC (graphiques compagnons,
// navigation temporelle, agrégation, axes gradués sur vue temps/valeur
// classique) est couvert par l'onglet Grafana, volontairement pas
// reconstruit ici.
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

function graduations(min, max, cible = 5) {
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

export default function Nomogramme({ type, mur, couche, position }) {
  const champs = CHAMPS_PAR_TYPE[type] ?? [];
  const [champX, setChampX] = useState(champs[0]?.valeur);
  const [champY, setChampY] = useState(champs[1]?.valeur ?? champs[0]?.valeur);
  const [points, setPoints] = useState([]);
  const [survol, setSurvol] = useState(null);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);
  const canvasRef = useRef(null);

  useEffect(() => {
    if (champs.length && !champs.some((c) => c.valeur === champX)) setChampX(champs[0].valeur);
    if (champs.length && !champs.some((c) => c.valeur === champY)) setChampY(champs[Math.min(1, champs.length - 1)].valeur);
  }, [type]); // eslint-disable-line react-hooks/exhaustive-deps

  const charger = async () => {
    if (!champX || !champY) return;
    setEnCours(true);
    setErreur(null);
    try {
      const params = Object.fromEntries(
        Object.entries({ type, mur, couche, position, champ_x: champX, champ_y: champY }).filter(([, v]) => v),
      );
      const resultat = await api.croisement(params);
      setPoints((resultat?.points ?? []).filter((p) => p.x != null && p.y != null));
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  useEffect(() => {
    charger();
  }, [type, mur, couche, position, champX, champY]); // eslint-disable-line react-hooks/exhaustive-deps

  const bornes = useMemo(() => {
    if (points.length === 0) return null;
    const xs = points.map((p) => p.x);
    const ys = points.map((p) => p.y);
    return { xMin: Math.min(...xs), xMax: Math.max(...xs), yMin: Math.min(...ys), yMax: Math.max(...ys) };
  }, [points]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !bornes) return;
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;
    canvas.width = w * devicePixelRatio;
    canvas.height = h * devicePixelRatio;
    const ctx = canvas.getContext("2d");
    ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
    ctx.clearRect(0, 0, w, h);

    const marge = 50;
    const { xMin, xMax, yMin, yMax } = bornes;
    const padX = (xMax - xMin) * 0.08 || 1;
    const padY = (yMax - yMin) * 0.08 || 1;
    const x = (v) => marge + ((v - (xMin - padX)) / (xMax + padX - (xMin - padX) || 1)) * (w - marge - 20);
    const y = (v) => h - marge - ((v - (yMin - padY)) / (yMax + padY - (yMin - padY) || 1)) * (h - marge - 20);

    // Grille + graduations.
    ctx.strokeStyle = "#2a2e3a";
    ctx.fillStyle = "#a0a6b5";
    ctx.font = "11px system-ui";
    ctx.lineWidth = 1;
    for (const gx of graduations(xMin - padX, xMax + padX)) {
      ctx.beginPath();
      ctx.moveTo(x(gx), 10);
      ctx.lineTo(x(gx), h - marge);
      ctx.stroke();
      ctx.fillText(gx.toString(), x(gx) - 10, h - marge + 16);
    }
    for (const gy of graduations(yMin - padY, yMax + padY)) {
      ctx.beginPath();
      ctx.moveTo(marge, y(gy));
      ctx.lineTo(w - 20, y(gy));
      ctx.stroke();
      ctx.fillText(gy.toString(), 8, y(gy) + 4);
    }

    // Points, couleur = position temporelle (bleu = ancien, rouge = récent).
    const temps = points.map((p) => new Date(p.time).getTime());
    const [tMin, tMax] = [Math.min(...temps), Math.max(...temps)];
    points.forEach((p, i) => {
      const frac = tMax > tMin ? (temps[i] - tMin) / (tMax - tMin) : 0;
      const hue = 220 - frac * 220; // 220 (bleu) -> 0 (rouge)
      ctx.fillStyle = `hsl(${hue}, 80%, 60%)`;
      ctx.beginPath();
      ctx.arc(x(p.x), y(p.y), 3, 0, 2 * Math.PI);
      ctx.fill();
    });

    // Lecture par projection au survol : lignes pointillées vers les axes.
    if (survol) {
      const px = x(survol.x);
      const py = y(survol.y);
      ctx.strokeStyle = "#7fd4ff";
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(px, py);
      ctx.lineTo(px, h - marge);
      ctx.moveTo(px, py);
      ctx.lineTo(marge, py);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = "#7fd4ff";
      ctx.beginPath();
      ctx.arc(px, py, 4, 0, 2 * Math.PI);
      ctx.fill();
      ctx.fillStyle = "#0f1117";
      ctx.fillRect(px + 8, py - 28, 130, 34);
      ctx.strokeStyle = "#7fd4ff";
      ctx.strokeRect(px + 8, py - 28, 130, 34);
      ctx.fillStyle = "#e6e6e6";
      ctx.fillText(`x = ${survol.x.toFixed(2)}`, px + 14, py - 14);
      ctx.fillText(`y = ${survol.y.toFixed(2)}`, px + 14, py - 2);
    }
  }, [points, bornes, survol]);

  const survolerCanvas = (e) => {
    if (points.length === 0 || !bornes) return;
    const canvas = canvasRef.current;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const w = canvas.clientWidth, h = canvas.clientHeight;
    const marge = 50;
    const { xMin, xMax, yMin, yMax } = bornes;
    const padX = (xMax - xMin) * 0.08 || 1;
    const padY = (yMax - yMin) * 0.08 || 1;
    const x = (v) => marge + ((v - (xMin - padX)) / (xMax + padX - (xMin - padX) || 1)) * (w - marge - 20);
    const y = (v) => h - marge - ((v - (yMin - padY)) / (yMax + padY - (yMin - padY) || 1)) * (h - marge - 20);

    let plusProche = null;
    let distanceMin = Infinity;
    for (const p of points) {
      const d = (x(p.x) - mx) ** 2 + (y(p.y) - my) ** 2;
      if (d < distanceMin) {
        distanceMin = d;
        plusProche = p;
      }
    }
    setSurvol(distanceMin < 400 ? plusProche : null);
  };

  if (champs.length === 0) return null;

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
      </div>
      {erreur && <p className="erreur">{erreur}</p>}
      {enCours && <p style={{ color: "#a0a6b5" }}>Chargement...</p>}
      {!enCours && points.length === 0 && !erreur && <p style={{ color: "#a0a6b5" }}>Aucun point croisé pour cette sélection.</p>}
      <canvas
        ref={canvasRef}
        style={{ width: "100%", height: "420px" }}
        onMouseMove={survolerCanvas}
        onMouseLeave={() => setSurvol(null)}
      />
    </div>
  );
}
