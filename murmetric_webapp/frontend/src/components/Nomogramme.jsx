import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api.js";
import { AXES_DISPONIBLES, CANAUX_RETRAIT, TYPES_TRACE, UNITES_TEMPS, construireParamAxe, libelleGrandeur, trouverCroisements } from "../nomogrammeAxes.js";

// Portage scopé du "nomogramme" de l'ancien POC (data_reel_compile/
// abaque-3d-hygrothermique.html, cf. logique_projet.md section 32) : le
// cœur unique (croiser deux grandeurs l'une contre l'autre, pas contre le
// temps par défaut — ce que Grafana ne fait pas nativement, sauf à choisir
// "Temps" comme axe) et la lecture de valeur par projection (lignes
// pointillées vers les axes au survol). Même catalogue d'axes que le
// nomogramme 3D (nomogrammeAxes.js, demande explicite du 13/08/2026) :
// grandeurs HR/T et retrait mélangeables, plus l'axe "Temps" avec unité
// configurable.
const ROLES = ["x", "y"];
const CLES_BACKEND = ["axe_x", "axe_y"];

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

export default function Nomogramme({ mur, couche }) {
  const [axeX, setAxeX] = useState("hr_t:temperature");
  const [axeY, setAxeY] = useState("hr_t:humidite");
  const [canal, setCanal] = useState("HA1");
  const [uniteTemps, setUniteTemps] = useState("jour");
  const [typeTrace, setTypeTrace] = useState("nuage");
  const [points, setPoints] = useState([]);
  const [survol, setSurvol] = useState(null);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);
  const [valeurCibleX, setValeurCibleX] = useState("");
  const [valeurCibleY, setValeurCibleY] = useState("");
  const canvasRef = useRef(null);

  const choixParRole = { x: axeX, y: axeY };
  const necessiteCanal = ROLES.some((r) => choixParRole[r].startsWith("retrait"));
  const necessiteUniteTemps = ROLES.some((r) => choixParRole[r] === "temps");

  function libelleAxe(role) {
    if (choixParRole[role] === "temps") return `Temps (${UNITES_TEMPS[uniteTemps].label})`;
    return libelleGrandeur(choixParRole[role]);
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
        params[CLES_BACKEND[i]] = construireParamAxe(choixParRole[role], canal);
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
          valeurs[role] = p[["x", "y"][idx]];
        });
        ROLES.forEach((role) => {
          if (choixParRole[role] === "temps") valeurs[role] = (tempsMs[i] - tMinMs) / diviseur;
        });
        return { time: p.time, x: valeurs.x, y: valeurs.y };
      });
      setPoints(finaux.filter((p) => p.x != null && p.y != null));
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  useEffect(() => {
    charger();
  }, [mur, couche, axeX, axeY, canal, uniteTemps]); // eslint-disable-line react-hooks/exhaustive-deps

  const bornes = useMemo(() => {
    if (points.length === 0) return null;
    const xs = points.map((p) => p.x);
    const ys = points.map((p) => p.y);
    return { xMin: Math.min(...xs), xMax: Math.max(...xs), yMin: Math.min(...ys), yMax: Math.max(...ys) };
  }, [points]);

  // Lecture par projection façon POC : on choisit une valeur cible sur un
  // axe, on trouve où la trajectoire la croise et on lit l'autre axe par
  // interpolation — dans les deux sens (x→y et y→x), pas seulement au
  // survol d'un point déjà présent.
  const croisementsX = useMemo(() => {
    const v = parseFloat(valeurCibleX);
    return Number.isNaN(v) ? [] : trouverCroisements(points, "x", v, ["y"]);
  }, [points, valeurCibleX]);
  const croisementsY = useMemo(() => {
    const v = parseFloat(valeurCibleY);
    return Number.isNaN(v) ? [] : trouverCroisements(points, "y", v, ["x"]);
  }, [points, valeurCibleY]);

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
    ctx.fillText(libelleAxe("x"), w - 90, h - marge + 30);
    ctx.save();
    ctx.translate(14, marge - 4);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText(libelleAxe("y"), 0, 0);
    ctx.restore();

    // Couleur = position temporelle (bleu = ancien, rouge = récent), pour
    // le trait comme pour les points.
    const temps = points.map((p) => new Date(p.time).getTime());
    const [tMin, tMax] = [Math.min(...temps), Math.max(...temps)];
    const teinte = (i) => 220 - (tMax > tMin ? (temps[i] - tMin) / (tMax - tMin) : 0) * 220;

    // Trait fin : relie les points dans l'ordre chronologique (déjà l'ordre
    // renvoyé par le backend) — dessiné avant les points pour qu'il reste
    // "dessous" en mode nuage + trait.
    if (typeTrace !== "nuage" && points.length > 1) {
      ctx.lineWidth = 1;
      for (let i = 0; i < points.length - 1; i++) {
        ctx.strokeStyle = `hsl(${teinte(i)}, 70%, 55%)`;
        ctx.beginPath();
        ctx.moveTo(x(points[i].x), y(points[i].y));
        ctx.lineTo(x(points[i + 1].x), y(points[i + 1].y));
        ctx.stroke();
      }
    }
    if (typeTrace !== "trait") {
      points.forEach((p, i) => {
        ctx.fillStyle = `hsl(${teinte(i)}, 80%, 60%)`;
        ctx.beginPath();
        ctx.arc(x(p.x), y(p.y), 3, 0, 2 * Math.PI);
        ctx.fill();
      });
    }

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
      ctx.fillRect(px + 8, py - 28, 150, 34);
      ctx.strokeStyle = "#7fd4ff";
      ctx.strokeRect(px + 8, py - 28, 150, 34);
      ctx.fillStyle = "#e6e6e6";
      ctx.fillText(`${libelleAxe("x")} = ${survol.x.toFixed(2)}`, px + 14, py - 14);
      ctx.fillText(`${libelleAxe("y")} = ${survol.y.toFixed(2)}`, px + 14, py - 2);
    }

    // Croisements demandés explicitement (x→y en vert, y→x en orange) —
    // peuvent coexister avec le survol ci-dessus.
    const dessinerCroisement = (px, py, couleur) => {
      ctx.strokeStyle = couleur;
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(px, py);
      ctx.lineTo(px, h - marge);
      ctx.moveTo(px, py);
      ctx.lineTo(marge, py);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = couleur;
      ctx.beginPath();
      ctx.arc(px, py, 4, 0, 2 * Math.PI);
      ctx.fill();
    };
    croisementsX.forEach((c) => dessinerCroisement(x(c.x), y(c.y), "#7fff9e"));
    croisementsY.forEach((c) => dessinerCroisement(x(c.x), y(c.y), "#ffb37f"));
  }, [points, bornes, survol, axeX, axeY, uniteTemps, typeTrace, croisementsX, croisementsY]); // eslint-disable-line react-hooks/exhaustive-deps

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
        <div className="champ">
          <label>Type de tracé</label>
          <select value={typeTrace} onChange={(e) => setTypeTrace(e.target.value)}>
            {TYPES_TRACE.map((t) => <option key={t.valeur} value={t.valeur}>{t.label}</option>)}
          </select>
        </div>
      </div>
      <div className="selection-form" style={{ marginBottom: "0.75rem" }}>
        <div className="champ">
          <label>Trouver {libelleAxe("y")} pour {libelleAxe("x")} =</label>
          <input value={valeurCibleX} onChange={(e) => setValeurCibleX(e.target.value)} placeholder="ex. 20" />
        </div>
        <div className="champ">
          <label>Trouver {libelleAxe("x")} pour {libelleAxe("y")} =</label>
          <input value={valeurCibleY} onChange={(e) => setValeurCibleY(e.target.value)} placeholder="ex. 65" />
        </div>
      </div>
      {(croisementsX.length > 0 || croisementsY.length > 0) && (
        <div style={{ fontSize: "0.85rem", maxHeight: "160px", overflowY: "auto", marginBottom: "0.75rem" }}>
          {croisementsX.length > 0 && (
            <p style={{ color: "#a0a6b5", margin: "0 0 0.25rem" }}>
              {croisementsX.length} croisement(s) à {libelleAxe("x")} = {valeurCibleX} :
            </p>
          )}
          {croisementsX.map((c, i) => (
            <div key={`x${i}`} style={{ color: "#7fff9e" }}>
              {libelleAxe("x")} = {valeurCibleX} · {libelleAxe("y")} ≈ {c.y.toFixed(2)}
            </div>
          ))}
          {croisementsY.length > 0 && (
            <p style={{ color: "#a0a6b5", margin: "0.5rem 0 0.25rem" }}>
              {croisementsY.length} croisement(s) à {libelleAxe("y")} = {valeurCibleY} :
            </p>
          )}
          {croisementsY.map((c, i) => (
            <div key={`y${i}`} style={{ color: "#ffb37f" }}>
              {libelleAxe("x")} ≈ {c.x.toFixed(2)} · {libelleAxe("y")} = {valeurCibleY}
            </div>
          ))}
        </div>
      )}
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
