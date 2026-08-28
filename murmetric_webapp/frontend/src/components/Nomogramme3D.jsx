import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api.js";
import { useCanauxRetrait } from "../canauxRetrait.js";
import {
  AXES_DISPONIBLES,
  AXES_GRANDEURS,
  COULEURS_CANAUX_RETRAIT,
  TYPES_TRACE,
  UNITES_TEMPS,
  construireParamAxe,
  graduations,
  graduationsTemps,
  trouverCroisements,
} from "../nomogrammeAxes.js";
import BoutonsExport from "./BoutonsExport.jsx";

// Nomogramme 3D — composition libre d'axes entre HR/T et retrait, y compris
// le TEMPS comme axe à part entière (demandes explicites du 13/08/2026) +
// options de vue façon POC (rotation auto, vues préréglées). Catalogue
// d'axes partagé avec le nomogramme 2D (nomogrammeAxes.js — même liste
// dans les deux, demande explicite du 13/08/2026). Projection en
// perspective classique (rotation yaw/pitch puis division perspective) —
// même principe mathématique générique que le POC, réécrit proprement
// (cf. logique_projet.md section 32 : le POC lui-même, ~50 fonctions très
// couplées à son propre DOM, n'a pas été repris tel quel).

const VUES_PREREGLEES = {
  face: { yaw: 0, pitch: 0 },
  dessus: { yaw: 0, pitch: 1.45 },
  profil: { yaw: 1.5708, pitch: 0 },
  isometrique: { yaw: 0.6, pitch: 0.35 },
};

function projeter(nx, ny, nz, yaw, pitch, zoom, w, h) {
  const cy = Math.cos(yaw),
    sy = Math.sin(yaw);
  const cp = Math.cos(pitch),
    sp = Math.sin(pitch);
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
  for (let cy = -1; cy <= 1; cy += 2) for (let cz = -1; cz <= 1; cz += 2) SOMMETS_CUBE.push([cx, cy, cz]);
const ARETES_CUBE = [
  [0, 1],
  [0, 2],
  [0, 4],
  [3, 1],
  [3, 2],
  [3, 7],
  [5, 1],
  [5, 4],
  [5, 7],
  [6, 2],
  [6, 4],
  [6, 7],
];

const ROLES = ["x", "y", "z"];
const CLES_BACKEND = ["axe_x", "axe_y", "axe_z"];

export default function Nomogramme3D({ mur, couche }) {
  const [axeX, setAxeX] = useState("temps");
  const [axeY, setAxeY] = useState("hr_t:temperature");
  const [axeZ, setAxeZ] = useState("hr_t:humidite");
  const [canal, setCanal] = useState("HA1");
  const [uniteTemps, setUniteTemps] = useState("jour");
  const [typeTrace, setTypeTrace] = useState("nuage");
  const [points, setPoints] = useState([]);
  // Séries indépendantes par grandeur, pour le panneau temps (28/08/2026,
  // demande explicite) — cf. Nomogramme.jsx : `points` (croisement du
  // cube) ne garde que les instants communs aux 3 axes, ce qui écrase la
  // densité réelle d'une grandeur dense croisée avec une éparse. Chargées
  // séparément, chacune garde sa cadence réelle.
  const [serieX, setSerieX] = useState([]);
  const [serieY, setSerieY] = useState([]);
  const [serieZ, setSerieZ] = useState([]);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);
  const [survol, setSurvol] = useState(null);
  const [axeRef, setAxeRef] = useState("x");
  const [valeurCible, setValeurCible] = useState("");
  // Début/Fin (28/08/2026) — même correctif que Nomogramme.jsx : sans eux,
  // la fenêtre par défaut (30 jours dès qu'un axe retrait est impliqué) ne
  // recouvre jamais des données anciennes comme la teneur en eau.
  const [debut, setDebut] = useState("");
  const [fin, setFin] = useState("");
  // Panneau "évolution dans le temps" + personnalisation (28/08/2026,
  // extension des mêmes fonctionnalités que Nomogramme.jsx) — un seul
  // panneau à axe partagé ici (pas de variante double/triple échelle : 3
  // axes indépendants superposés serait illisible, contrairement au cas à
  // 2 courbes du nomogramme 2D).
  const [resolutionTemps, setResolutionTemps] = useState("jour");
  const [survolTemps, setSurvolTemps] = useState(null);
  const [couleurX, setCouleurX] = useState("#7fd4ff");
  const [couleurY, setCouleurY] = useState("#ffb37f");
  const [couleurZ, setCouleurZ] = useState("#c47fff"); // pas #7fff9e (déjà pris par le marqueur de croisement)
  const [couleurFond, setCouleurFond] = useState("#12141c");

  const [yaw, setYaw] = useState(VUES_PREREGLEES.isometrique.yaw);
  const [pitch, setPitch] = useState(VUES_PREREGLEES.isometrique.pitch);
  const [zoom, setZoom] = useState(1);
  const [rotationAuto, setRotationAuto] = useState(false);
  const canvasRef = useRef(null);
  const canvasTempsRef = useRef(null);
  const glisseRef = useRef(null);
  // Garde anti-concurrence (28/08/2026) — cf. Nomogramme.jsx : seule la
  // requête la plus récente au moment de la résolution met à jour l'état.
  const requeteIdRef = useRef(0);

  const choixParRole = { x: axeX, y: axeY, z: axeZ };
  const necessiteCanal = ROLES.some((r) => choixParRole[r].startsWith("retrait"));
  const necessiteUniteTemps = ROLES.some((r) => choixParRole[r] === "temps");
  // "Tous" = croisement RÉEL des 8 canaux superposés, pas une moyenne — cf.
  // Nomogramme.jsx (même correctif, 28/08/2026).
  const multiCanal = necessiteCanal && canal === "";
  // Canaux + moyennes filtrés sur le "Mur" déjà sélectionné plus haut dans
  // la page (28/08/2026, demande explicite) — cf. canauxRetrait.js.
  const { canaux: canauxDisponibles, moyennes: moyennesCanaux } = useCanauxRetrait(mur);

  function libelleAxe(role) {
    if (choixParRole[role] === "temps") return `Temps (${UNITES_TEMPS[uniteTemps].label})`;
    return AXES_GRANDEURS.find((a) => a.valeur === choixParRole[role])?.label ?? choixParRole[role];
  }

  // Récupère UNE SEULE grandeur (rôle "x"/"y"/"z"), SANS la croiser avec
  // les autres — cf. Nomogramme.jsx : croisement-libre appelé avec un seul
  // axe renvoie toute sa série, pas de filtrage par instant commun.
  const chargerSerieIndependante = async (role) => {
    const grandeur = choixParRole[role];
    if (grandeur === "temps") return [];
    const roleEstRetrait = grandeur.startsWith("retrait");
    const canauxAInterroger = roleEstRetrait ? (multiCanal ? canauxDisponibles : [canal]) : [null];
    const resultats = await Promise.all(
      canauxAInterroger.map(async (c) => {
        const params = { mur, couche, debut, fin, axe_x: construireParamAxe(grandeur, c) };
        Object.keys(params).forEach((k) => (params[k] == null || params[k] === "") && delete params[k]);
        const resultat = await api.croisementLibre(params);
        return { canal: c, bruts: resultat?.points ?? [] };
      }),
    );
    return resultats.flatMap(({ canal: canalPoint, bruts }) =>
      bruts.map((p) => ({
        time: p.time,
        valeur: p.x,
        canal: roleEstRetrait && multiCanal ? canalPoint : null,
      })),
    );
  };

  const charger = async () => {
    const idCourant = ++requeteIdRef.current;
    const rolesReels = ROLES.filter((r) => choixParRole[r] !== "temps");
    if (rolesReels.length === 0) {
      setErreur("Choisis au moins une grandeur réelle en plus du temps.");
      setPoints([]);
      setSerieX([]);
      setSerieY([]);
      setSerieZ([]);
      return;
    }
    setEnCours(true);
    setErreur(null);
    try {
      const canaux = multiCanal ? canauxDisponibles : [canal];
      const [resultats, nouvelleSerieX, nouvelleSerieY, nouvelleSerieZ] = await Promise.all([
        Promise.all(
          canaux.map(async (c) => {
            const params = { mur, couche, debut, fin };
            rolesReels.forEach((role, i) => {
              params[CLES_BACKEND[i]] = construireParamAxe(choixParRole[role], c);
            });
            Object.keys(params).forEach((k) => (params[k] == null || params[k] === "") && delete params[k]);
            const resultat = await api.croisementLibre(params);
            return { canal: c, bruts: resultat?.points ?? [] };
          }),
        ),
        chargerSerieIndependante("x"),
        chargerSerieIndependante("y"),
        chargerSerieIndependante("z"),
      ]);

      const diviseur = UNITES_TEMPS[uniteTemps].diviseur;
      const tousTempsMs = resultats.flatMap((r) => r.bruts.map((p) => new Date(p.time).getTime()));
      const tMinMs = tousTempsMs.length ? Math.min(...tousTempsMs) : 0;

      const finaux = resultats.flatMap(({ canal: canalPoint, bruts }) =>
        bruts.map((p) => {
          const tempsMs = new Date(p.time).getTime();
          const valeurs = {};
          rolesReels.forEach((role, idx) => {
            valeurs[role] = p[["x", "y", "z"][idx]];
          });
          ROLES.forEach((role) => {
            if (choixParRole[role] === "temps") valeurs[role] = (tempsMs - tMinMs) / diviseur;
          });
          return { time: p.time, x: valeurs.x, y: valeurs.y, z: valeurs.z, canal: multiCanal ? canalPoint : null };
        }),
      );
      if (requeteIdRef.current !== idCourant) return; // réponse obsolète, une requête plus récente est déjà partie
      setPoints(finaux.filter((p) => p.x != null && p.y != null && p.z != null));
      setSerieX(nouvelleSerieX);
      setSerieY(nouvelleSerieY);
      setSerieZ(nouvelleSerieZ);
    } catch (e) {
      if (requeteIdRef.current !== idCourant) return;
      setErreur(e.message);
      // Cf. Nomogramme.jsx — sans ça, un ancien tracé valide reste affiché
      // à côté du message d'erreur.
      setPoints([]);
      setSerieX([]);
      setSerieY([]);
      setSerieZ([]);
    } finally {
      if (requeteIdRef.current === idCourant) setEnCours(false);
    }
  };

  useEffect(() => {
    charger();
  }, [mur, couche, axeX, axeY, axeZ, canal, uniteTemps, debut, fin, canauxDisponibles]); // eslint-disable-line react-hooks/exhaustive-deps

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
    const xs = points.map((p) => p.x),
      ys = points.map((p) => p.y),
      zs = points.map((p) => p.z);
    return {
      xMin: Math.min(...xs),
      xMax: Math.max(...xs),
      yMin: Math.min(...ys),
      yMax: Math.max(...ys),
      zMin: Math.min(...zs),
      zMax: Math.max(...zs),
    };
  }, [points]);

  const normaliser = (p, b) => [
    b.xMax > b.xMin ? ((p.x - b.xMin) / (b.xMax - b.xMin)) * 2 - 1 : 0,
    b.yMax > b.yMin ? ((p.y - b.yMin) / (b.yMax - b.yMin)) * 2 - 1 : 0,
    b.zMax > b.zMin ? ((p.z - b.zMin) / (b.zMax - b.zMin)) * 2 - 1 : 0,
  ];

  // Regroupement par canal (multi-canaux uniquement) — cf. Nomogramme.jsx :
  // des points de canaux différents concaténés ne forment pas une
  // trajectoire continue, à traiter séparément pour le tracé et la lecture
  // par projection.
  const pointsParCanal = useMemo(() => {
    if (!multiCanal) return null;
    const groupes = {};
    points.forEach((p) => {
      (groupes[p.canal] ??= []).push(p);
    });
    return groupes;
  }, [points, multiCanal]);

  // Lecture par projection façon POC : choisir une valeur cible sur UN axe
  // de référence et lire les DEUX autres par interpolation le long de la
  // trajectoire, à chaque endroit où elle croise cette valeur. En
  // multi-canaux, calculé PAR CANAL puis fusionné, chacun étiqueté.
  const autresRoles = ROLES.filter((r) => r !== axeRef);
  const croisements = useMemo(() => {
    const v = parseFloat(valeurCible);
    if (Number.isNaN(v)) return [];
    if (pointsParCanal) {
      return Object.entries(pointsParCanal).flatMap(([c, pts]) =>
        trouverCroisements(pts, axeRef, v, autresRoles).map((cr) => ({ ...cr, canal: c })),
      );
    }
    return trouverCroisements(points, axeRef, v, autresRoles);
  }, [points, pointsParCanal, axeRef, valeurCible]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const w = canvas.clientWidth,
      h = canvas.clientHeight;
    canvas.width = w * devicePixelRatio;
    canvas.height = h * devicePixelRatio;
    const ctx = canvas.getContext("2d");
    ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = couleurFond;
    ctx.fillRect(0, 0, w, h);
    // Sortie APRÈS le clearRect (28/08/2026) — même correctif que
    // Nomogramme.jsx : sinon le dernier rendu valide reste figé à l'écran
    // dès qu'une sélection retourne 0 point.
    if (!bornes) return;

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
    const boutX = proj(1, -1, -1),
      boutY = proj(-1, 1, -1),
      boutZ = proj(-1, -1, 1);
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

    if (pointsParCanal) {
      // Multi-canaux : couleur fixe par canal au lieu du dégradé temporel
      // — cf. Nomogramme.jsx. Trait regroupé par canal (sur les points déjà
      // projetés, pour ne pas reprojeter) afin de ne jamais relier deux
      // canaux différents entre eux.
      if (typeTrace !== "nuage") {
        const projetesParCanal = {};
        projetes.forEach((pp) => {
          (projetesParCanal[pp.point.canal] ??= []).push(pp);
        });
        Object.entries(projetesParCanal).forEach(([c, pts]) => {
          if (pts.length < 2) return;
          ctx.strokeStyle = COULEURS_CANAUX_RETRAIT[c] || "#a0a6b5";
          ctx.lineWidth = 1;
          for (let i = 0; i < pts.length - 1; i++) {
            ctx.beginPath();
            ctx.moveTo(pts[i].x, pts[i].y);
            ctx.lineTo(pts[i + 1].x, pts[i + 1].y);
            ctx.stroke();
          }
        });
      }

      if (typeTrace !== "trait") {
        // Occlusion (algorithme du peintre) toujours calculée sur
        // l'ensemble combiné des canaux — un point plus proche de la
        // caméra doit occulter les autres, peu importe son canal.
        const parProfondeur = [...projetes].sort((a, b) => a.profondeur - b.profondeur);
        for (const pp of parProfondeur) {
          const estSurvole = survol && survol.point === pp.point;
          ctx.fillStyle = estSurvole ? "#ffffff" : COULEURS_CANAUX_RETRAIT[pp.point.canal] || "#a0a6b5";
          ctx.beginPath();
          ctx.arc(pp.x, pp.y, estSurvole ? 5 : 3, 0, 2 * Math.PI);
          ctx.fill();
        }
      } else if (survol) {
        const pp = projetes.find((p) => p.point === survol.point);
        if (pp) {
          ctx.fillStyle = "#ffffff";
          ctx.beginPath();
          ctx.arc(pp.x, pp.y, 5, 0, 2 * Math.PI);
          ctx.fill();
        }
      }

      // Légende — un carré de couleur + le nom du canal, en haut à droite.
      ctx.font = "11px system-ui";
      Object.keys(pointsParCanal)
        .sort()
        .forEach((c, i) => {
          const ly = 14 + i * 16;
          ctx.fillStyle = COULEURS_CANAUX_RETRAIT[c] || "#a0a6b5";
          ctx.fillRect(w - 66, ly, 10, 10);
          ctx.fillStyle = "#e6e6e6";
          ctx.fillText(c, w - 52, ly + 9);
        });
    } else {
      // Trait fin : relie les points dans l'ordre chronologique (ordre
      // d'arrivée, PAS l'ordre trié par profondeur ci-dessous — la
      // trajectoire suit le temps, pas l'occlusion).
      if (typeTrace !== "nuage" && projetes.length > 1) {
        ctx.lineWidth = 1;
        for (let i = 0; i < projetes.length - 1; i++) {
          ctx.strokeStyle = `hsl(${220 - projetes[i].frac * 220}, 70%, 55%)`;
          ctx.beginPath();
          ctx.moveTo(projetes[i].x, projetes[i].y);
          ctx.lineTo(projetes[i + 1].x, projetes[i + 1].y);
          ctx.stroke();
        }
      }

      // Points, triés par profondeur pour une occlusion correcte
      // (algorithme du peintre) — inutile pour le trait, qui suit sa
      // propre logique temporelle ci-dessus.
      if (typeTrace !== "trait") {
        const parProfondeur = [...projetes].sort((a, b) => a.profondeur - b.profondeur);
        for (const pp of parProfondeur) {
          const hue = 220 - pp.frac * 220;
          const estSurvole = survol && survol.point === pp.point;
          ctx.fillStyle = estSurvole ? "#ffffff" : `hsl(${hue}, 80%, 60%)`;
          ctx.beginPath();
          ctx.arc(pp.x, pp.y, estSurvole ? 5 : 3, 0, 2 * Math.PI);
          ctx.fill();
        }
      } else if (survol) {
        // Mode "trait" seul : pas de points dessinés, mais on garde un
        // marqueur au survol pour la lecture de valeur.
        const pp = projetes.find((p) => p.point === survol.point);
        if (pp) {
          ctx.fillStyle = "#ffffff";
          ctx.beginPath();
          ctx.arc(pp.x, pp.y, 5, 0, 2 * Math.PI);
          ctx.fill();
        }
      }
    }

    if (survol) {
      const p = survol.point;
      // Boîte dimensionnée sur le texte réel (28/08/2026) — cf.
      // Nomogramme.jsx : une largeur fixe débordait avec un libellé long.
      const lignesInfobulle = [];
      if (p.canal) lignesInfobulle.push(`Canal ${p.canal}`);
      lignesInfobulle.push(`${libelleAxe("x")} = ${p.x.toFixed(2)}`);
      lignesInfobulle.push(`${libelleAxe("y")} = ${p.y.toFixed(2)}`);
      lignesInfobulle.push(`${libelleAxe("z")} = ${p.z.toFixed(2)}`);
      ctx.font = "11px system-ui";
      const largeurBoite = Math.max(...lignesInfobulle.map((l) => ctx.measureText(l).width)) + 20;
      const hauteurBoite = 10 + lignesInfobulle.length * 12;
      ctx.fillStyle = "#0f1117";
      ctx.fillRect(survol.x + 8, survol.y - hauteurBoite + 6, largeurBoite, hauteurBoite);
      ctx.strokeStyle = "#7fd4ff";
      ctx.strokeRect(survol.x + 8, survol.y - hauteurBoite + 6, largeurBoite, hauteurBoite);
      ctx.fillStyle = "#e6e6e6";
      lignesInfobulle.forEach((ligne, i) => {
        ctx.fillText(ligne, survol.x + 14, survol.y - hauteurBoite + 20 + i * 12);
      });
    }

    // Croisements demandés explicitement — marqueurs verts sur la
    // trajectoire, aux endroits où elle passe par la valeur cible choisie.
    // Halo blanc si le croisement est le point actuellement survolé
    // (28/08/2026, correctif) — jusqu'ici, la comparaison d'identité
    // `survol.point === pp.point` ne portait que sur `projetes` (dérivé de
    // `points`), jamais égale à un objet croisement même quand c'est bien
    // lui le point survolé (l'infobulle, elle, fonctionnait déjà —
    // uniquement le retour visuel sur le marqueur lui-même manquait).
    for (const c of croisements) {
      const [nx, ny, nz] = normaliser(c, bornes);
      const pp = proj(nx, ny, nz);
      const estSurvole = survol && survol.point === c;
      const couleur = estSurvole ? "#ffffff" : "#7fff9e";
      ctx.fillStyle = couleur;
      ctx.beginPath();
      ctx.arc(pp.x, pp.y, estSurvole ? 7 : 5, 0, 2 * Math.PI);
      ctx.fill();
      ctx.strokeStyle = couleur;
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.arc(pp.x, pp.y, estSurvole ? 11 : 9, 0, 2 * Math.PI);
      ctx.stroke();
    }
  }, [
    points,
    pointsParCanal,
    bornes,
    yaw,
    pitch,
    zoom,
    survol,
    axeX,
    axeY,
    axeZ,
    uniteTemps,
    typeTrace,
    croisements,
    couleurFond,
  ]); // eslint-disable-line react-hooks/exhaustive-deps

  // Panneau "évolution dans le temps" (28/08/2026, extension 3D du même
  // panneau construit pour Nomogramme.jsx) — un axe Y partagé pour les
  // grandeurs réellement choisies parmi X/Y/Z (excluant "temps" s'il est
  // déjà l'un des 3 axes du cube), contre le temps calendaire réel — donc
  // jamais totalement redondant avec le cube même quand "Temps" y figure
  // déjà : cet axe-là est un temps ÉCOULÉ depuis le premier point choisi
  // par l'utilisateur, pas des dates réelles.
  useEffect(() => {
    const canvas = canvasTempsRef.current;
    if (!canvas) return;
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;
    canvas.width = w * devicePixelRatio;
    canvas.height = h * devicePixelRatio;
    const ctx = canvas.getContext("2d");
    ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
    ctx.clearRect(0, 0, w, h);
    ctx.fillStyle = couleurFond;
    ctx.fillRect(0, 0, w, h);

    const SERIES_ROLE = { x: serieX, y: serieY, z: serieZ };
    const COULEURS_ROLE = { x: couleurX, y: couleurY, z: couleurZ };
    const rolesPresents = ROLES.filter((r) => SERIES_ROLE[r].length > 0);
    if (rolesPresents.length === 0) return;

    const toutesLesSeries = rolesPresents.flatMap((r) => SERIES_ROLE[r]);
    const temps = toutesLesSeries.map((p) => new Date(p.time).getTime());
    const tMin = Math.min(...temps);
    const tMax = Math.max(...temps);
    const valeurs = toutesLesSeries.map((p) => p.valeur).filter((v) => v != null);
    const vMin = Math.min(...valeurs);
    const vMax = Math.max(...valeurs);
    const padV = (vMax - vMin) * 0.08 || 1;

    const marge = 50;
    const tx = (t) => marge + ((t - tMin) / (tMax - tMin || 1)) * (w - marge - 20);
    const ty = (v) => h - marge - ((v - (vMin - padV)) / (vMax + padV - (vMin - padV) || 1)) * (h - marge - 20);

    ctx.strokeStyle = "#2a2e3a";
    ctx.fillStyle = "#a0a6b5";
    ctx.font = "11px system-ui";
    ctx.lineWidth = 1;
    for (const gv of graduations(vMin - padV, vMax + padV)) {
      ctx.beginPath();
      ctx.moveTo(marge, ty(gv));
      ctx.lineTo(w - 20, ty(gv));
      ctx.stroke();
      ctx.fillText(gv.toString(), 8, ty(gv) + 4);
    }
    for (const tick of graduationsTemps(tMin, tMax, resolutionTemps)) {
      const px = tx(tick.t);
      ctx.strokeStyle = "#2a2e3a";
      ctx.beginPath();
      ctx.moveTo(px, 10);
      ctx.lineTo(px, h - marge);
      ctx.stroke();
      ctx.fillStyle = "#a0a6b5";
      ctx.fillText(tick.label, px - 15, h - marge + 16);
    }
    ctx.fillText("Temps", w - 40, h - marge + 34);
    ctx.save();
    ctx.translate(14, marge - 4);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText("Valeur", 0, 0);
    ctx.restore();

    // Regroupement par canal pour la continuité des courbes — cf.
    // Nomogramme.jsx : des points de canaux différents concaténés ne
    // forment pas une trajectoire continue. `canal` vaut null hors mode
    // "Tous les canaux" : un seul groupe naturel dans ce cas.
    const grouperParCanal = (serie) => {
      const groupes = {};
      serie.forEach((p) => {
        (groupes[p.canal] ??= []).push(p);
      });
      return Object.values(groupes);
    };
    rolesPresents.forEach((role) => {
      const couleur = COULEURS_ROLE[role];
      ctx.strokeStyle = couleur;
      ctx.fillStyle = couleur;
      ctx.lineWidth = 1.5;
      grouperParCanal(SERIES_ROLE[role]).forEach((pts) => {
        const tries = [...pts].sort((a, b) => new Date(a.time) - new Date(b.time));
        if (typeTrace !== "nuage" && tries.length > 1) {
          ctx.beginPath();
          tries.forEach((p, i) => {
            const px = tx(new Date(p.time).getTime());
            const py = ty(p.valeur);
            if (i === 0) ctx.moveTo(px, py);
            else ctx.lineTo(px, py);
          });
          ctx.stroke();
        }
        if (typeTrace !== "trait") {
          tries.forEach((p) => {
            ctx.beginPath();
            ctx.arc(tx(new Date(p.time).getTime()), ty(p.valeur), 2.5, 0, 2 * Math.PI);
            ctx.fill();
          });
        }
      });
    });

    // Légende — seulement pour les grandeurs réellement chargées (une
    // grandeur "Temps" n'a pas de série, cf. chargerSerieIndependante).
    ctx.font = "11px system-ui";
    rolesPresents.forEach((role, i) => {
      const ly = 14 + i * 16;
      ctx.fillStyle = COULEURS_ROLE[role];
      ctx.fillRect(w - 90, ly, 10, 10);
      ctx.fillStyle = "#e6e6e6";
      ctx.fillText(libelleAxe(role), w - 76, ly + 9);
    });

    // Croisement demandé explicitement, replacé dans le temps — cf.
    // Nomogramme.jsx point 4 (même principe, une seule couleur ici : la
    // recherche 3D porte sur un seul axe de référence, pas deux sens
    // séparés comme en 2D).
    croisements.forEach((c) => {
      if (c.time == null) return;
      const px = tx(new Date(c.time).getTime());
      ctx.strokeStyle = "#7fff9e";
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(px, 10);
      ctx.lineTo(px, h - marge);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = "#7fff9e";
      rolesPresents.forEach((role) => {
        if (c[role] == null) return;
        ctx.beginPath();
        ctx.arc(px, ty(c[role]), 5, 0, 2 * Math.PI);
        ctx.fill();
      });
    });

    // Infobulle au survol — le point survolé peut ne porter qu'UNE seule
    // valeur (série X, Y ou Z seule) ou plusieurs (croisement), donc chaque
    // ligne/marqueur devient conditionnel.
    if (survolTemps) {
      const px = tx(new Date(survolTemps.time).getTime());
      ctx.strokeStyle = "#7fd4ff";
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(px, 10);
      ctx.lineTo(px, h - marge);
      ctx.stroke();
      ctx.setLineDash([]);
      const positionsY = [];
      rolesPresents.forEach((role) => {
        if (survolTemps[role] == null) return;
        ctx.fillStyle = COULEURS_ROLE[role];
        ctx.beginPath();
        ctx.arc(px, ty(survolTemps[role]), 4, 0, 2 * Math.PI);
        ctx.fill();
        positionsY.push(ty(survolTemps[role]));
      });

      const lignesInfobulle = [];
      if (survolTemps.canal) lignesInfobulle.push(`Canal ${survolTemps.canal}`);
      lignesInfobulle.push(new Date(survolTemps.time).toLocaleString("fr-FR"));
      rolesPresents.forEach((role) => {
        if (survolTemps[role] != null) lignesInfobulle.push(`${libelleAxe(role)} = ${survolTemps[role].toFixed(2)}`);
      });
      ctx.font = "11px system-ui";
      const largeurBoite = Math.max(...lignesInfobulle.map((l) => ctx.measureText(l).width)) + 20;
      const hauteurBoite = 10 + lignesInfobulle.length * 12;
      const pyAncrage = positionsY.reduce((a, b) => a + b, 0) / positionsY.length;
      const boiteX = px + largeurBoite + 16 > w ? px - largeurBoite - 8 : px + 8;
      ctx.fillStyle = "#0f1117";
      ctx.fillRect(boiteX, pyAncrage - hauteurBoite / 2, largeurBoite, hauteurBoite);
      ctx.strokeStyle = "#7fd4ff";
      ctx.strokeRect(boiteX, pyAncrage - hauteurBoite / 2, largeurBoite, hauteurBoite);
      ctx.fillStyle = "#e6e6e6";
      lignesInfobulle.forEach((ligne, i) => {
        ctx.fillText(ligne, boiteX + 6, pyAncrage - hauteurBoite / 2 + 14 + i * 12);
      });
    }
  }, [
    serieX,
    serieY,
    serieZ,
    axeX,
    axeY,
    axeZ,
    typeTrace,
    resolutionTemps,
    survolTemps,
    couleurX,
    couleurY,
    couleurZ,
    couleurFond,
    croisements,
  ]); // eslint-disable-line react-hooks/exhaustive-deps

  const survolerCanvasTemps = (e) => {
    if (serieX.length === 0 && serieY.length === 0 && serieZ.length === 0) return;
    const canvas = canvasTempsRef.current;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const w = canvas.clientWidth;
    const marge = 50;
    const toutesLesSeries = [...serieX, ...serieY, ...serieZ];
    const temps = toutesLesSeries.map((p) => new Date(p.time).getTime());
    const tMin = Math.min(...temps);
    const tMax = Math.max(...temps);
    const tCible = tMin + ((mx - marge) / (w - marge - 20)) * (tMax - tMin || 1);
    // Croisements inclus dans la recherche — cf. Nomogramme.jsx. Chaque
    // candidat normalisé en { time, x?, y?, z?, canal } pour un rendu
    // d'infobulle uniforme, qu'il vienne d'une série seule ou d'un croisement.
    const candidats = [
      ...serieX.map((p) => ({ time: p.time, x: p.valeur, canal: p.canal })),
      ...serieY.map((p) => ({ time: p.time, y: p.valeur, canal: p.canal })),
      ...serieZ.map((p) => ({ time: p.time, z: p.valeur, canal: p.canal })),
      ...croisements.filter((c) => c.time != null),
    ];
    let plusProche = null;
    let distanceMin = Infinity;
    for (const p of candidats) {
      const d = Math.abs(new Date(p.time).getTime() - tCible);
      if (d < distanceMin) {
        distanceMin = d;
        plusProche = p;
      }
    }
    setSurvolTemps(plusProche && distanceMin < (tMax - tMin || 1) * 0.02 ? plusProche : null);
  };

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
    const mx = e.clientX - rect.left,
      my = e.clientY - rect.top;
    const w = canvas.clientWidth,
      h = canvas.clientHeight;
    // Croisements inclus dans la recherche — cf. survolerCanvasTemps.
    const candidats = [...points, ...croisements];
    let plusProche = null,
      distanceMin = Infinity,
      meilleurProj = null;
    for (const p of candidats) {
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
            {AXES_DISPONIBLES.map((a) => (
              <option key={a.valeur} value={a.valeur}>
                {a.label}
              </option>
            ))}
          </select>
        </div>
        <div className="champ">
          <label>Axe Y</label>
          <select value={axeY} onChange={(e) => setAxeY(e.target.value)}>
            {AXES_DISPONIBLES.map((a) => (
              <option key={a.valeur} value={a.valeur}>
                {a.label}
              </option>
            ))}
          </select>
        </div>
        <div className="champ">
          <label>Axe Z</label>
          <select value={axeZ} onChange={(e) => setAxeZ(e.target.value)}>
            {AXES_DISPONIBLES.map((a) => (
              <option key={a.valeur} value={a.valeur}>
                {a.label}
              </option>
            ))}
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
              <option value="">Tous</option>
              <optgroup label="Canal individuel">
                {canauxDisponibles.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </optgroup>
              {moyennesCanaux.length > 0 && (
                <optgroup label="Moyenne (même mur, même orientation)">
                  {moyennesCanaux.map((m) => (
                    <option key={m.valeur} value={m.valeur}>
                      {m.label}
                    </option>
                  ))}
                </optgroup>
              )}
            </select>
          </div>
        )}
        <div className="champ">
          <label>Type de tracé</label>
          <select value={typeTrace} onChange={(e) => setTypeTrace(e.target.value)}>
            {TYPES_TRACE.map((t) => (
              <option key={t.valeur} value={t.valeur}>
                {t.label}
              </option>
            ))}
          </select>
        </div>
        <div className="champ">
          <label>Résolution axe temps</label>
          <select value={resolutionTemps} onChange={(e) => setResolutionTemps(e.target.value)}>
            <option value="jour">Jour</option>
            <option value="semaine">Semaine</option>
            <option value="mois">Mois</option>
            <option value="annee">Année</option>
          </select>
        </div>
        <div className="champ">
          <label>Début</label>
          <input type="date" value={debut} onChange={(e) => setDebut(e.target.value)} />
        </div>
        <div className="champ">
          <label>Fin</label>
          <input type="date" value={fin} onChange={(e) => setFin(e.target.value)} />
        </div>
        <div className="champ">
          <label>Couleur X</label>
          <input type="color" value={couleurX} onChange={(e) => setCouleurX(e.target.value)} />
        </div>
        <div className="champ">
          <label>Couleur Y</label>
          <input type="color" value={couleurY} onChange={(e) => setCouleurY(e.target.value)} />
        </div>
        <div className="champ">
          <label>Couleur Z</label>
          <input type="color" value={couleurZ} onChange={(e) => setCouleurZ(e.target.value)} />
        </div>
        <div className="champ">
          <label>Fond des graphiques</label>
          <input type="color" value={couleurFond} onChange={(e) => setCouleurFond(e.target.value)} />
        </div>
      </div>

      <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", flexWrap: "wrap", marginBottom: "0.5rem" }}>
        <button type="button" onClick={() => appliquerVue("face")}>
          Face
        </button>
        <button type="button" onClick={() => appliquerVue("dessus")}>
          Dessus
        </button>
        <button type="button" onClick={() => appliquerVue("profil")}>
          Profil
        </button>
        <button type="button" onClick={() => appliquerVue("isometrique")}>
          Isométrique
        </button>
        <button type="button" onClick={() => setRotationAuto((v) => !v)}>
          {rotationAuto ? "⏸ Arrêter la rotation" : "▶ Rotation automatique"}
        </button>
        <button type="button" onClick={() => setZoom(1)}>
          Réinitialiser le zoom
        </button>
        {!multiCanal && (
          <div
            style={{
              marginLeft: "auto",
              display: "flex",
              alignItems: "center",
              gap: "0.4rem",
              fontSize: "0.78rem",
              color: "#a0a6b5",
            }}
          >
            <span>ancien</span>
            <div
              style={{
                width: "60px",
                height: "8px",
                borderRadius: "4px",
                background: "linear-gradient(90deg, hsl(220,80%,60%), hsl(0,80%,60%))",
              }}
            />
            <span>récent</span>
          </div>
        )}
      </div>
      <p style={{ color: "#a0a6b5", fontSize: "0.8rem", margin: "0 0 0.5rem" }}>
        Glisser pour tourner · molette pour zoomer · survoler un point pour lire ses 3 valeurs.
      </p>

      <div className="selection-form" style={{ marginBottom: "0.5rem" }}>
        <div className="champ">
          <label>Axe de référence</label>
          <select value={axeRef} onChange={(e) => setAxeRef(e.target.value)}>
            <option value="x">{libelleAxe("x")} (axe X)</option>
            <option value="y">{libelleAxe("y")} (axe Y)</option>
            <option value="z">{libelleAxe("z")} (axe Z)</option>
          </select>
        </div>
        <div className="champ">
          <label>Valeur cible</label>
          <input value={valeurCible} onChange={(e) => setValeurCible(e.target.value)} placeholder="ex. 20" />
        </div>
      </div>
      {croisements.length > 0 && (
        <p style={{ fontSize: "0.85rem", color: "#7fff9e" }}>
          {croisements.map((c, i) => (
            <span key={i} style={{ marginRight: "1rem" }}>
              {c.canal ? `[${c.canal}] ` : ""}
              {autresRoles.map((r) => `${libelleAxe(r)} ≈ ${c[r].toFixed(2)}`).join(" · ")}
            </span>
          ))}
        </p>
      )}
      {erreur && <p className="erreur">{erreur}</p>}
      {enCours && <p style={{ color: "#a0a6b5" }}>Chargement...</p>}
      {!enCours && points.length === 0 && !erreur && (
        <p style={{ color: "#a0a6b5" }}>Aucun point croisé pour cette sélection.</p>
      )}
      <canvas
        ref={canvasRef}
        style={{ width: "100%", height: "480px", cursor: glisseRef.current ? "grabbing" : "grab" }}
        onMouseDown={surSourisBas}
        onMouseMove={surSourisDeplace}
        onMouseUp={surSourisHaut}
        onMouseLeave={surSourisHaut}
        onWheel={surMolette}
      />
      {points.length > 0 && (
        <BoutonsExport obtenirElement={() => canvasRef.current} type="canvas" nomFichier="nomogramme-3d" />
      )}
      <div style={{ marginTop: "1rem" }}>
        <p style={{ color: "#a0a6b5", fontSize: "0.8rem", margin: "0 0 0.25rem" }}>
          Évolution dans le temps —{" "}
          {ROLES.filter((r) => choixParRole[r] !== "temps")
            .map(libelleAxe)
            .join(", ")}
        </p>
        <canvas
          ref={canvasTempsRef}
          style={{ width: "100%", height: "420px" }}
          onMouseMove={survolerCanvasTemps}
          onMouseLeave={() => setSurvolTemps(null)}
        />
        {(serieX.length > 0 || serieY.length > 0 || serieZ.length > 0) && (
          <BoutonsExport obtenirElement={() => canvasTempsRef.current} type="canvas" nomFichier="nomogramme-3d-temps" />
        )}
      </div>
    </div>
  );
}
