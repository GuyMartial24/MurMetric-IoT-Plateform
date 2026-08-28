import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api.js";
import { useCanauxRetrait } from "../canauxRetrait.js";
import {
  AXES_DISPONIBLES,
  COULEURS_CANAUX_RETRAIT,
  TYPES_TRACE,
  UNITES_TEMPS,
  construireParamAxe,
  graduations,
  graduationsTemps,
  libelleGrandeur,
  trouverCroisements,
} from "../nomogrammeAxes.js";
import BoutonsExport from "./BoutonsExport.jsx";

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

export default function Nomogramme({ mur, couche }) {
  const [axeX, setAxeX] = useState("hr_t:temperature");
  const [axeY, setAxeY] = useState("hr_t:humidite");
  const [canal, setCanal] = useState("HA1");
  const [uniteTemps, setUniteTemps] = useState("jour");
  const [typeTrace, setTypeTrace] = useState("nuage");
  const [points, setPoints] = useState([]);
  // Séries indépendantes par grandeur, pour les panneaux 2/3 (28/08/2026,
  // demande explicite) — `points` (croisement) ne garde que les instants
  // communs aux 2 axes ; avec une grandeur éparse (ex. teneur en eau)
  // croisée avec une dense (ex. retrait), l'intersection écrase la densité
  // réelle de la grandeur dense, donnant l'impression fausse que les deux
  // sont aussi rares l'une que l'autre. Chargées séparément, chacune garde
  // sa cadence réelle.
  const [serieX, setSerieX] = useState([]);
  const [serieY, setSerieY] = useState([]);
  const [survol, setSurvol] = useState(null);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);
  const [valeurCibleX, setValeurCibleX] = useState("");
  const [valeurCibleY, setValeurCibleY] = useState("");
  // Début/Fin (28/08/2026) — sans eux, la fenêtre par défaut du backend
  // (30 jours dès qu'un axe retrait est impliqué, cf. mesures.py) ne
  // recouvre jamais les données anciennes (ex. teneur en eau, backfill
  // jusqu'à mars 2026) : croisement structurellement impossible malgré un
  // mur/couche valides, symptôme signalé par l'utilisateur le 28/08/2026.
  const [debut, setDebut] = useState("");
  const [fin, setFin] = useState("");
  // Résolution de l'axe temps des panneaux 2/3 (28/08/2026, demande
  // explicite) — distincte de "Unité de temps" ci-dessus, qui sert à un
  // usage différent (convertir "Temps" en grandeur d'axe du croisement).
  const [resolutionTemps, setResolutionTemps] = useState("jour");
  const [survolTemps, setSurvolTemps] = useState(null);
  const [survolDouble, setSurvolDouble] = useState(null);
  // Couleurs personnalisables (28/08/2026, demande explicite) — couleurX/Y
  // pilotent les 2 courbes des panneaux 2/3 (le croisement du panneau 1 ne
  // s'y prête pas de la même façon : il colore par ancienneté temporelle
  // ou par canal, pas par grandeur X/Y). couleurFond s'applique aux 3
  // canevas, dessiné explicitement (ils étaient transparents jusqu'ici,
  // simplement posés sur le fond de la page).
  const [couleurX, setCouleurX] = useState("#7fd4ff");
  const [couleurY, setCouleurY] = useState("#ffb37f");
  const [couleurFond, setCouleurFond] = useState("#12141c");
  const canvasRef = useRef(null);
  const canvasDroiteRef = useRef(null);
  const canvasDoubleEchelleRef = useRef(null);
  // Garde anti-concurrence (28/08/2026) : chaque changement de Début/Fin
  // déclenche un nouveau chargement sans annuler le précédent — une
  // réponse plus ancienne mais plus rapide (ou une erreur plus ancienne)
  // pouvait arriver APRÈS une réponse plus récente et écraser son
  // résultat, mélangeant un tracé périmé avec l'erreur de la requête
  // courante (constaté en direct). Seule la requête la plus récente au
  // moment de la résolution est autorisée à mettre à jour l'état.
  const requeteIdRef = useRef(0);

  const choixParRole = { x: axeX, y: axeY };
  const necessiteCanal = ROLES.some((r) => choixParRole[r].startsWith("retrait"));
  const necessiteUniteTemps = ROLES.some((r) => choixParRole[r] === "temps");
  // "Tous" (canal vide) = croisement RÉEL des 8 canaux, chacun sa propre
  // trajectoire superposée — pas une moyenne (cf. discussion utilisateur du
  // 28/08/2026). Un seul appel par canal en parallèle, chaque point
  // étiqueté avec son canal d'origine ; comportement/rendu à 1 seul canal
  // strictement inchangé sinon (canal: null, pas de couleur fixe imposée).
  const multiCanal = necessiteCanal && canal === "";
  // Canaux + moyennes filtrés sur le "Mur" déjà sélectionné plus haut dans
  // la page (28/08/2026, demande explicite) — cf. canauxRetrait.js.
  const { canaux: canauxDisponibles, moyennes: moyennesCanaux } = useCanauxRetrait(mur);

  function libelleAxe(role) {
    if (choixParRole[role] === "temps") return `Temps (${UNITES_TEMPS[uniteTemps].label})`;
    return libelleGrandeur(choixParRole[role]);
  }

  // Récupère UNE SEULE grandeur (rôle "x" ou "y" de choixParRole), SANS la
  // croiser avec l'autre — en appelant croisement-libre avec un seul axe,
  // l'intersection (qui ne porte alors que sur une série) renvoie tous ses
  // points, pas de filtrage par instant commun. "temps" n'a rien à
  // récupérer (axe virtuel, calculé côté client) — série vide.
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
      return;
    }
    setEnCours(true);
    setErreur(null);
    try {
      const canaux = multiCanal ? canauxDisponibles : [canal];
      const [resultats, nouvelleSerieX, nouvelleSerieY] = await Promise.all([
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
      ]);

      // tMin calculé sur l'ensemble combiné des canaux, pour un axe Temps
      // cohérent d'une trajectoire à l'autre plutôt que remis à zéro par canal.
      const diviseur = UNITES_TEMPS[uniteTemps].diviseur;
      const tousTempsMs = resultats.flatMap((r) => r.bruts.map((p) => new Date(p.time).getTime()));
      const tMinMs = tousTempsMs.length ? Math.min(...tousTempsMs) : 0;

      const finaux = resultats.flatMap(({ canal: canalPoint, bruts }) =>
        bruts.map((p) => {
          const tempsMs = new Date(p.time).getTime();
          const valeurs = {};
          rolesReels.forEach((role, idx) => {
            valeurs[role] = p[["x", "y"][idx]];
          });
          ROLES.forEach((role) => {
            if (choixParRole[role] === "temps") valeurs[role] = (tempsMs - tMinMs) / diviseur;
          });
          return { time: p.time, x: valeurs.x, y: valeurs.y, canal: multiCanal ? canalPoint : null };
        }),
      );
      if (requeteIdRef.current !== idCourant) return; // réponse obsolète, une requête plus récente est déjà partie
      setPoints(finaux.filter((p) => p.x != null && p.y != null));
      setSerieX(nouvelleSerieX);
      setSerieY(nouvelleSerieY);
    } catch (e) {
      if (requeteIdRef.current !== idCourant) return;
      setErreur(e.message);
      // Sans ça, un ancien tracé valide reste affiché à côté du message
      // d'erreur (28/08/2026) — laisse croire que la requête en échec a
      // quand même renvoyé quelque chose.
      setPoints([]);
      setSerieX([]);
      setSerieY([]);
    } finally {
      if (requeteIdRef.current === idCourant) setEnCours(false);
    }
  };

  useEffect(() => {
    charger();
  }, [mur, couche, axeX, axeY, canal, uniteTemps, debut, fin, canauxDisponibles]); // eslint-disable-line react-hooks/exhaustive-deps

  const bornes = useMemo(() => {
    if (points.length === 0) return null;
    const xs = points.map((p) => p.x);
    const ys = points.map((p) => p.y);
    return { xMin: Math.min(...xs), xMax: Math.max(...xs), yMin: Math.min(...ys), yMax: Math.max(...ys) };
  }, [points]);

  // Regroupement par canal (multi-canaux uniquement) — nécessaire pour la
  // lecture par projection ET pour le tracé : des points de canaux
  // différents concaténés dans un seul tableau ne forment PAS une
  // trajectoire continue, les traiter comme telle créerait des segments
  // fictifs entre le dernier point d'un canal et le premier du suivant.
  const pointsParCanal = useMemo(() => {
    if (!multiCanal) return null;
    const groupes = {};
    points.forEach((p) => {
      (groupes[p.canal] ??= []).push(p);
    });
    return groupes;
  }, [points, multiCanal]);

  // Lecture par projection façon POC : on choisit une valeur cible sur un
  // axe, on trouve où la trajectoire la croise et on lit l'autre axe par
  // interpolation — dans les deux sens (x→y et y→x), pas seulement au
  // survol d'un point déjà présent. En multi-canaux, croisements calculés
  // PAR CANAL (cf. pointsParCanal) puis fusionnés, chacun étiqueté.
  const croisementsX = useMemo(() => {
    const v = parseFloat(valeurCibleX);
    if (Number.isNaN(v)) return [];
    if (pointsParCanal) {
      return Object.entries(pointsParCanal).flatMap(([c, pts]) =>
        trouverCroisements(pts, "x", v, ["y"]).map((cr) => ({ ...cr, canal: c })),
      );
    }
    return trouverCroisements(points, "x", v, ["y"]);
  }, [points, pointsParCanal, valeurCibleX]);
  const croisementsY = useMemo(() => {
    const v = parseFloat(valeurCibleY);
    if (Number.isNaN(v)) return [];
    if (pointsParCanal) {
      return Object.entries(pointsParCanal).flatMap(([c, pts]) =>
        trouverCroisements(pts, "y", v, ["x"]).map((cr) => ({ ...cr, canal: c })),
      );
    }
    return trouverCroisements(points, "y", v, ["x"]);
  }, [points, pointsParCanal, valeurCibleY]);

  useEffect(() => {
    const canvas = canvasRef.current;
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
    // Sortie APRÈS le clearRect (28/08/2026, correctif) : sinon une
    // sélection qui retourne 0 point (ex. axes sans recouvrement temporel)
    // laisse le dernier tracé valide affiché indéfiniment — y compris son
    // ancien type de tracé, qui semblait alors "ne pas se mettre à jour"
    // en changeant nuage/trait puisque cet effet ne se ré-exécutait jamais.
    if (!bornes) return;

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

    if (pointsParCanal) {
      // Multi-canaux : une couleur fixe par canal (pointsParCanal garantit
      // que chaque trajectoire reste continue, cf. sa définition) plutôt
      // que le dégradé temporel ci-dessous, qui n'a plus de sens dès qu'on
      // superpose plusieurs trajectoires distinctes.
      Object.entries(pointsParCanal).forEach(([c, pts]) => {
        const couleur = COULEURS_CANAUX_RETRAIT[c] || "#a0a6b5";
        if (typeTrace !== "nuage" && pts.length > 1) {
          ctx.strokeStyle = couleur;
          ctx.lineWidth = 1;
          for (let i = 0; i < pts.length - 1; i++) {
            ctx.beginPath();
            ctx.moveTo(x(pts[i].x), y(pts[i].y));
            ctx.lineTo(x(pts[i + 1].x), y(pts[i + 1].y));
            ctx.stroke();
          }
        }
        if (typeTrace !== "trait") {
          ctx.fillStyle = couleur;
          pts.forEach((p) => {
            ctx.beginPath();
            ctx.arc(x(p.x), y(p.y), 3, 0, 2 * Math.PI);
            ctx.fill();
          });
        }
      });

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
      // Couleur = position temporelle (bleu = ancien, rouge = récent), pour
      // le trait comme pour les points.
      const temps = points.map((p) => new Date(p.time).getTime());
      const [tMin, tMax] = [Math.min(...temps), Math.max(...temps)];
      const teinte = (i) => 220 - (tMax > tMin ? (temps[i] - tMin) / (tMax - tMin) : 0) * 220;

      // Trait fin : relie les points dans l'ordre chronologique (déjà
      // l'ordre renvoyé par le backend) — dessiné avant les points pour
      // qu'il reste "dessous" en mode nuage + trait.
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
      // Boîte dimensionnée sur le texte réel (28/08/2026) — une largeur
      // fixe (150px à l'origine) débordait dès qu'un libellé d'axe était
      // un peu long (ex. "Teneur en eau (%)"), rendant la valeur illisible.
      const lignesInfobulle = [];
      if (survol.canal) lignesInfobulle.push(`Canal ${survol.canal}`);
      lignesInfobulle.push(`${libelleAxe("x")} = ${survol.x.toFixed(2)}`);
      lignesInfobulle.push(`${libelleAxe("y")} = ${survol.y.toFixed(2)}`);
      ctx.font = "11px system-ui";
      const largeurBoite = Math.max(...lignesInfobulle.map((l) => ctx.measureText(l).width)) + 20;
      const hauteurBoite = 10 + lignesInfobulle.length * 12;
      ctx.fillStyle = "#0f1117";
      ctx.fillRect(px + 8, py - hauteurBoite + 6, largeurBoite, hauteurBoite);
      ctx.strokeStyle = "#7fd4ff";
      ctx.strokeRect(px + 8, py - hauteurBoite + 6, largeurBoite, hauteurBoite);
      ctx.fillStyle = "#e6e6e6";
      lignesInfobulle.forEach((ligne, i) => {
        ctx.fillText(ligne, px + 14, py - hauteurBoite + 20 + i * 12);
      });
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
  }, [
    points,
    pointsParCanal,
    bornes,
    survol,
    axeX,
    axeY,
    uniteTemps,
    typeTrace,
    croisementsX,
    croisementsY,
    couleurFond,
  ]); // eslint-disable-line react-hooks/exhaustive-deps

  // Panneau de droite (28/08/2026, demande explicite) : X et Y ne sont plus
  // croisés l'un contre l'autre mais tracés chacun individuellement contre
  // le TEMPS, superposés dans un même repère — complète le croisement de
  // gauche (qui élimine le temps) en montrant l'évolution de chaque
  // grandeur. Un seul axe Y partagé pour les deux courbes, sans double
  // échelle (choix confirmé malgré des échelles réelles différentes — les
  // 3 panneaux sont gardés en parallèle, cf. discussion utilisateur du
  // 28/08/2026). Graduations temps + infobulle au survol ajoutées le même
  // jour (initialement absentes, "première version").
  useEffect(() => {
    const canvas = canvasDroiteRef.current;
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
    if (serieX.length === 0 && serieY.length === 0) return;

    const temps = [...serieX, ...serieY].map((p) => new Date(p.time).getTime());
    const tMin = Math.min(...temps);
    const tMax = Math.max(...temps);
    const valeurs = [...serieX, ...serieY].map((p) => p.valeur).filter((v) => v != null);
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
      ctx.beginPath();
      ctx.moveTo(px, 10);
      ctx.lineTo(px, h - marge);
      ctx.stroke();
      ctx.fillText(tick.label, px - 15, h - marge + 16);
    }
    ctx.fillText("Temps", w - 40, h - marge + 34);
    ctx.save();
    ctx.translate(14, marge - 4);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText("Valeur", 0, 0);
    ctx.restore();

    // Regroupement par canal pour la continuité des courbes — sans lui, un
    // tracé "trait" relierait à tort le dernier point d'un canal au
    // premier du suivant. `canal` vaut null hors mode "Tous les canaux" :
    // un seul groupe naturel dans ce cas, pas de traitement spécial requis.
    const COULEUR_X = couleurX;
    const COULEUR_Y = couleurY;
    const grouperParCanal = (serie) => {
      const groupes = {};
      serie.forEach((p) => {
        (groupes[p.canal] ??= []).push(p);
      });
      return Object.values(groupes);
    };
    // Type de tracé (28/08/2026, correctif) — respecte désormais nuage/
    // trait/nuage_trait comme le panneau de croisement, au lieu d'afficher
    // systématiquement points + trait quel que soit le choix.
    const tracerCourbe = (groupes, couleur) => {
      ctx.strokeStyle = couleur;
      ctx.fillStyle = couleur;
      ctx.lineWidth = 1.5;
      groupes.forEach((pts) => {
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
    };
    if (serieX.length > 0) tracerCourbe(grouperParCanal(serieX), COULEUR_X);
    if (serieY.length > 0) tracerCourbe(grouperParCanal(serieY), COULEUR_Y);

    // Légende — seulement pour les grandeurs réellement chargées (une
    // grandeur "Temps" n'a pas de série, cf. chargerSerieIndependante).
    ctx.font = "11px system-ui";
    let ligneLegende = 14;
    if (serieX.length > 0) {
      ctx.fillStyle = COULEUR_X;
      ctx.fillRect(w - 90, ligneLegende, 10, 10);
      ctx.fillStyle = "#e6e6e6";
      ctx.fillText(libelleAxe("x"), w - 76, ligneLegende + 9);
      ligneLegende += 16;
    }
    if (serieY.length > 0) {
      ctx.fillStyle = COULEUR_Y;
      ctx.fillRect(w - 90, ligneLegende, 10, 10);
      ctx.fillStyle = "#e6e6e6";
      ctx.fillText(libelleAxe("y"), w - 76, ligneLegende + 9);
    }

    // Croisements demandés explicitement (28/08/2026, demande explicite) —
    // replace le point trouvé sur le croisement de gauche dans le temps
    // réel : un seul repère vertical (l'instant interpolé), avec les deux
    // valeurs X/Y marquées sur leurs courbes respectives à cet instant.
    const dessinerCroisementTemps = (c, couleur) => {
      if (c.time == null) return;
      const px = tx(new Date(c.time).getTime());
      ctx.strokeStyle = couleur;
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(px, 10);
      ctx.lineTo(px, h - marge);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = couleur;
      [ty(c.x), ty(c.y)].forEach((py) => {
        ctx.beginPath();
        ctx.arc(px, py, 5, 0, 2 * Math.PI);
        ctx.fill();
      });
    };
    croisementsX.forEach((c) => dessinerCroisementTemps(c, "#7fff9e"));
    croisementsY.forEach((c) => dessinerCroisementTemps(c, "#ffb37f"));

    // Infobulle au survol (28/08/2026) — le point survolé peut venir de la
    // série X seule, Y seule, ou d'un croisement (les deux) : `.x`/`.y`
    // absents selon le cas, chaque ligne/marqueur devient donc conditionnel.
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
      if (survolTemps.x != null) {
        ctx.fillStyle = COULEUR_X;
        ctx.beginPath();
        ctx.arc(px, ty(survolTemps.x), 4, 0, 2 * Math.PI);
        ctx.fill();
        positionsY.push(ty(survolTemps.x));
      }
      if (survolTemps.y != null) {
        ctx.fillStyle = COULEUR_Y;
        ctx.beginPath();
        ctx.arc(px, ty(survolTemps.y), 4, 0, 2 * Math.PI);
        ctx.fill();
        positionsY.push(ty(survolTemps.y));
      }

      const lignesInfobulle = [];
      if (survolTemps.canal) lignesInfobulle.push(`Canal ${survolTemps.canal}`);
      lignesInfobulle.push(new Date(survolTemps.time).toLocaleString("fr-FR"));
      if (survolTemps.x != null) lignesInfobulle.push(`${libelleAxe("x")} = ${survolTemps.x.toFixed(2)}`);
      if (survolTemps.y != null) lignesInfobulle.push(`${libelleAxe("y")} = ${survolTemps.y.toFixed(2)}`);
      ctx.font = "11px system-ui";
      const largeurBoite = Math.max(...lignesInfobulle.map((l) => ctx.measureText(l).width)) + 20;
      const hauteurBoite = 10 + lignesInfobulle.length * 12;
      const pyMoyen = positionsY.reduce((a, b) => a + b, 0) / positionsY.length;
      const boiteX = px + largeurBoite + 16 > w ? px - largeurBoite - 8 : px + 8;
      ctx.fillStyle = "#0f1117";
      ctx.fillRect(boiteX, pyMoyen - hauteurBoite / 2, largeurBoite, hauteurBoite);
      ctx.strokeStyle = "#7fd4ff";
      ctx.strokeRect(boiteX, pyMoyen - hauteurBoite / 2, largeurBoite, hauteurBoite);
      ctx.fillStyle = "#e6e6e6";
      lignesInfobulle.forEach((ligne, i) => {
        ctx.fillText(ligne, boiteX + 6, pyMoyen - hauteurBoite / 2 + 14 + i * 12);
      });
    }
  }, [
    serieX,
    serieY,
    axeX,
    axeY,
    typeTrace,
    resolutionTemps,
    survolTemps,
    couleurX,
    couleurY,
    couleurFond,
    croisementsX,
    croisementsY,
  ]); // eslint-disable-line react-hooks/exhaustive-deps

  const survolerCanvasTemps = (e) => {
    if (serieX.length === 0 && serieY.length === 0) return;
    const canvas = canvasDroiteRef.current;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const w = canvas.clientWidth;
    const marge = 50;
    const temps = [...serieX, ...serieY].map((p) => new Date(p.time).getTime());
    const tMin = Math.min(...temps);
    const tMax = Math.max(...temps);
    const tCible = tMin + ((mx - marge) / (w - marge - 20)) * (tMax - tMin || 1);
    // Croisements inclus dans la recherche — sans ça, l'infobulle ne se
    // déclenchait que sur un vrai point mesuré, jamais sur le point
    // interpolé trouvé par "Trouver X/Y pour...". Chaque candidat normalisé
    // en { time, x?, y?, canal } pour un rendu d'infobulle uniforme.
    const candidats = [
      ...serieX.map((p) => ({ time: p.time, x: p.valeur, canal: p.canal })),
      ...serieY.map((p) => ({ time: p.time, y: p.valeur, canal: p.canal })),
      ...croisementsX.filter((c) => c.time != null),
      ...croisementsY.filter((c) => c.time != null),
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
    // Seuil relatif à l'étendue totale plutôt qu'en pixels — évite un
    // survol qui "colle" partout sur une plage très resserrée.
    setSurvolTemps(plusProche && distanceMin < (tMax - tMin || 1) * 0.02 ? plusProche : null);
  };

  // 3e panneau, à double échelle (28/08/2026, demande explicite) — même
  // évolution X(t)/Y(t) que le panneau du milieu, mais avec un axe Y par
  // grandeur (échelle propre à chacune) plutôt qu'un axe partagé, pour
  // comparer lequel des deux se lit mieux en pratique. Grille dessinée
  // seulement pour l'axe gauche (X) : deux grilles superposées auraient
  // surchargé visuellement le graphique ; l'axe droit (Y) garde ses
  // repères chiffrés, teintés dans sa couleur pour rester rattachable à sa
  // courbe.
  useEffect(() => {
    const canvas = canvasDoubleEchelleRef.current;
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
    if (serieX.length === 0 && serieY.length === 0) return;

    const temps = [...serieX, ...serieY].map((p) => new Date(p.time).getTime());
    const tMin = Math.min(...temps);
    const tMax = Math.max(...temps);
    const xs = serieX.map((p) => p.valeur).filter((v) => v != null);
    const ys = serieY.map((p) => p.valeur).filter((v) => v != null);
    const xMin = Math.min(...xs);
    const xMax = Math.max(...xs);
    const yMin = Math.min(...ys);
    const yMax = Math.max(...ys);
    const padX = (xMax - xMin) * 0.08 || 1;
    const padY = (yMax - yMin) * 0.08 || 1;

    const margeG = 50;
    const margeD = 50;
    const margeBas = 50;
    const tx = (t) => margeG + ((t - tMin) / (tMax - tMin || 1)) * (w - margeG - margeD);
    const tyX = (v) => h - margeBas - ((v - (xMin - padX)) / (xMax + padX - (xMin - padX) || 1)) * (h - margeBas - 20);
    const tyY = (v) => h - margeBas - ((v - (yMin - padY)) / (yMax + padY - (yMin - padY) || 1)) * (h - margeBas - 20);

    const COULEUR_X = couleurX;
    const COULEUR_Y = couleurY;

    ctx.font = "11px system-ui";
    ctx.strokeStyle = "#2a2e3a";
    ctx.lineWidth = 1;
    if (serieX.length > 0) {
      for (const gv of graduations(xMin - padX, xMax + padX)) {
        ctx.beginPath();
        ctx.moveTo(margeG, tyX(gv));
        ctx.lineTo(w - margeD, tyX(gv));
        ctx.stroke();
        ctx.fillStyle = COULEUR_X;
        ctx.fillText(gv.toString(), 8, tyX(gv) + 4);
      }
    }
    if (serieY.length > 0) {
      for (const gv of graduations(yMin - padY, yMax + padY)) {
        ctx.fillStyle = COULEUR_Y;
        ctx.fillText(gv.toString(), w - margeD + 8, tyY(gv) + 4);
      }
    }
    ctx.fillStyle = "#a0a6b5";
    for (const tick of graduationsTemps(tMin, tMax, resolutionTemps)) {
      const px = tx(tick.t);
      ctx.strokeStyle = "#2a2e3a";
      ctx.beginPath();
      ctx.moveTo(px, 10);
      ctx.lineTo(px, h - margeBas);
      ctx.stroke();
      ctx.fillStyle = "#a0a6b5";
      ctx.fillText(tick.label, px - 15, h - margeBas + 16);
    }
    ctx.fillText("Temps", w / 2 - 20, h - margeBas + 34);
    if (serieX.length > 0) {
      ctx.save();
      ctx.translate(14, h / 2 + 20);
      ctx.rotate(-Math.PI / 2);
      ctx.fillStyle = COULEUR_X;
      ctx.fillText(libelleAxe("x"), 0, 0);
      ctx.restore();
    }
    if (serieY.length > 0) {
      ctx.save();
      ctx.translate(w - 14, h / 2 - 20);
      ctx.rotate(Math.PI / 2);
      ctx.fillStyle = COULEUR_Y;
      ctx.fillText(libelleAxe("y"), 0, 0);
      ctx.restore();
    }

    // Regroupement par canal pour la continuité des courbes — cf. panneau
    // du milieu. `canal` vaut null hors mode "Tous les canaux" : un seul
    // groupe naturel dans ce cas.
    const grouperParCanal = (serie) => {
      const groupes = {};
      serie.forEach((p) => {
        (groupes[p.canal] ??= []).push(p);
      });
      return Object.values(groupes);
    };
    // Type de tracé (28/08/2026, correctif) — cf. panneau du milieu.
    const tracerCourbe = (groupes, echelle, couleur) => {
      ctx.strokeStyle = couleur;
      ctx.fillStyle = couleur;
      ctx.lineWidth = 1.5;
      groupes.forEach((pts) => {
        const tries = [...pts].sort((a, b) => new Date(a.time) - new Date(b.time));
        if (typeTrace !== "nuage" && tries.length > 1) {
          ctx.beginPath();
          tries.forEach((p, i) => {
            const px = tx(new Date(p.time).getTime());
            const py = echelle(p.valeur);
            if (i === 0) ctx.moveTo(px, py);
            else ctx.lineTo(px, py);
          });
          ctx.stroke();
        }
        if (typeTrace !== "trait") {
          tries.forEach((p) => {
            ctx.beginPath();
            ctx.arc(tx(new Date(p.time).getTime()), echelle(p.valeur), 2.5, 0, 2 * Math.PI);
            ctx.fill();
          });
        }
      });
    };
    if (serieX.length > 0) tracerCourbe(grouperParCanal(serieX), tyX, COULEUR_X);
    if (serieY.length > 0) tracerCourbe(grouperParCanal(serieY), tyY, COULEUR_Y);

    // Croisements demandés explicitement — cf. panneau du milieu, chaque
    // valeur positionnée sur SON propre axe (tyX/tyY).
    const dessinerCroisementTemps = (c, couleur) => {
      if (c.time == null) return;
      const px = tx(new Date(c.time).getTime());
      ctx.strokeStyle = couleur;
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(px, 10);
      ctx.lineTo(px, h - margeBas);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = couleur;
      [tyX(c.x), tyY(c.y)].forEach((py) => {
        ctx.beginPath();
        ctx.arc(px, py, 5, 0, 2 * Math.PI);
        ctx.fill();
      });
    };
    croisementsX.forEach((c) => dessinerCroisementTemps(c, "#7fff9e"));
    croisementsY.forEach((c) => dessinerCroisementTemps(c, "#ffb37f"));

    // Infobulle au survol — cf. panneau du milieu, adaptée aux 2 échelles
    // indépendantes (chaque valeur reste positionnée sur SON propre axe) et
    // au fait que le point survolé peut ne porter que x OU y (série seule).
    if (survolDouble) {
      const px = tx(new Date(survolDouble.time).getTime());
      ctx.strokeStyle = "#7fd4ff";
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(px, 10);
      ctx.lineTo(px, h - margeBas);
      ctx.stroke();
      ctx.setLineDash([]);
      const positionsY = [];
      if (survolDouble.x != null) {
        ctx.fillStyle = COULEUR_X;
        ctx.beginPath();
        ctx.arc(px, tyX(survolDouble.x), 4, 0, 2 * Math.PI);
        ctx.fill();
        positionsY.push(tyX(survolDouble.x));
      }
      if (survolDouble.y != null) {
        ctx.fillStyle = COULEUR_Y;
        ctx.beginPath();
        ctx.arc(px, tyY(survolDouble.y), 4, 0, 2 * Math.PI);
        ctx.fill();
        positionsY.push(tyY(survolDouble.y));
      }

      const lignesInfobulle = [];
      if (survolDouble.canal) lignesInfobulle.push(`Canal ${survolDouble.canal}`);
      lignesInfobulle.push(new Date(survolDouble.time).toLocaleString("fr-FR"));
      if (survolDouble.x != null) lignesInfobulle.push(`${libelleAxe("x")} = ${survolDouble.x.toFixed(2)}`);
      if (survolDouble.y != null) lignesInfobulle.push(`${libelleAxe("y")} = ${survolDouble.y.toFixed(2)}`);
      ctx.font = "11px system-ui";
      const largeurBoite = Math.max(...lignesInfobulle.map((l) => ctx.measureText(l).width)) + 20;
      const hauteurBoite = 10 + lignesInfobulle.length * 12;
      const pyMoyen = positionsY.reduce((a, b) => a + b, 0) / positionsY.length;
      const boiteX = px + largeurBoite + 16 > w ? px - largeurBoite - 8 : px + 8;
      ctx.fillStyle = "#0f1117";
      ctx.fillRect(boiteX, pyMoyen - hauteurBoite / 2, largeurBoite, hauteurBoite);
      ctx.strokeStyle = "#7fd4ff";
      ctx.strokeRect(boiteX, pyMoyen - hauteurBoite / 2, largeurBoite, hauteurBoite);
      ctx.fillStyle = "#e6e6e6";
      lignesInfobulle.forEach((ligne, i) => {
        ctx.fillText(ligne, boiteX + 6, pyMoyen - hauteurBoite / 2 + 14 + i * 12);
      });
    }
  }, [
    serieX,
    serieY,
    axeX,
    axeY,
    typeTrace,
    resolutionTemps,
    survolDouble,
    couleurX,
    couleurY,
    couleurFond,
    croisementsX,
    croisementsY,
  ]); // eslint-disable-line react-hooks/exhaustive-deps

  const survolerCanvasDouble = (e) => {
    if (serieX.length === 0 && serieY.length === 0) return;
    const canvas = canvasDoubleEchelleRef.current;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const w = canvas.clientWidth;
    const margeG = 50;
    const margeD = 50;
    const temps = [...serieX, ...serieY].map((p) => new Date(p.time).getTime());
    const tMin = Math.min(...temps);
    const tMax = Math.max(...temps);
    const tCible = tMin + ((mx - margeG) / (w - margeG - margeD)) * (tMax - tMin || 1);
    // Croisements inclus dans la recherche — cf. survolerCanvasTemps.
    const candidats = [
      ...serieX.map((p) => ({ time: p.time, x: p.valeur, canal: p.canal })),
      ...serieY.map((p) => ({ time: p.time, y: p.valeur, canal: p.canal })),
      ...croisementsX.filter((c) => c.time != null),
      ...croisementsY.filter((c) => c.time != null),
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
    setSurvolDouble(plusProche && distanceMin < (tMax - tMin || 1) * 0.02 ? plusProche : null);
  };

  const survolerCanvas = (e) => {
    if (points.length === 0 || !bornes) return;
    const canvas = canvasRef.current;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const w = canvas.clientWidth,
      h = canvas.clientHeight;
    const marge = 50;
    const { xMin, xMax, yMin, yMax } = bornes;
    const padX = (xMax - xMin) * 0.08 || 1;
    const padY = (yMax - yMin) * 0.08 || 1;
    const x = (v) => marge + ((v - (xMin - padX)) / (xMax + padX - (xMin - padX) || 1)) * (w - marge - 20);
    const y = (v) => h - marge - ((v - (yMin - padY)) / (yMax + padY - (yMin - padY) || 1)) * (h - marge - 20);

    // Croisements inclus dans la recherche — cf. survolerCanvasTemps.
    const candidats = [...points, ...croisementsX, ...croisementsY];
    let plusProche = null;
    let distanceMin = Infinity;
    for (const p of candidats) {
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
          <label>Fond des graphiques</label>
          <input type="color" value={couleurFond} onChange={(e) => setCouleurFond(e.target.value)} />
        </div>
      </div>
      <div className="selection-form" style={{ marginBottom: "0.75rem" }}>
        <div className="champ">
          <label>
            Trouver {libelleAxe("y")} pour {libelleAxe("x")} =
          </label>
          <input value={valeurCibleX} onChange={(e) => setValeurCibleX(e.target.value)} placeholder="ex. 20" />
        </div>
        <div className="champ">
          <label>
            Trouver {libelleAxe("x")} pour {libelleAxe("y")} =
          </label>
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
              {c.canal ? `[${c.canal}] ` : ""}
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
              {c.canal ? `[${c.canal}] ` : ""}
              {libelleAxe("x")} ≈ {c.x.toFixed(2)} · {libelleAxe("y")} = {valeurCibleY}
            </div>
          ))}
        </div>
      )}
      {erreur && <p className="erreur">{erreur}</p>}
      {enCours && <p style={{ color: "#a0a6b5" }}>Chargement...</p>}
      {!enCours && points.length === 0 && !erreur && (
        <p style={{ color: "#a0a6b5" }}>Aucun point croisé pour cette sélection.</p>
      )}
      <div style={{ display: "flex", gap: "1rem", flexWrap: "wrap" }}>
        <div style={{ flex: "1 1 400px", minWidth: 0 }}>
          <p style={{ color: "#a0a6b5", fontSize: "0.8rem", margin: "0 0 0.25rem" }}>
            Croisement {libelleAxe("y")} = f({libelleAxe("x")})
          </p>
          <canvas
            ref={canvasRef}
            style={{ width: "100%", height: "420px" }}
            onMouseMove={survolerCanvas}
            onMouseLeave={() => setSurvol(null)}
          />
          {points.length > 0 && (
            <BoutonsExport obtenirElement={() => canvasRef.current} type="canvas" nomFichier="nomogramme-2d" />
          )}
        </div>
        <div style={{ flex: "1 1 400px", minWidth: 0 }}>
          <p style={{ color: "#a0a6b5", fontSize: "0.8rem", margin: "0 0 0.25rem" }}>
            Évolution dans le temps — {libelleAxe("x")} et {libelleAxe("y")}
          </p>
          <canvas
            ref={canvasDroiteRef}
            style={{ width: "100%", height: "420px" }}
            onMouseMove={survolerCanvasTemps}
            onMouseLeave={() => setSurvolTemps(null)}
          />
          {points.length > 0 && (
            <BoutonsExport
              obtenirElement={() => canvasDroiteRef.current}
              type="canvas"
              nomFichier="nomogramme-2d-temps"
            />
          )}
        </div>
      </div>
      <div style={{ marginTop: "1rem" }}>
        <p style={{ color: "#a0a6b5", fontSize: "0.8rem", margin: "0 0 0.25rem" }}>
          Évolution dans le temps — double échelle (comparaison, {libelleAxe("x")} à gauche, {libelleAxe("y")} à droite)
        </p>
        <canvas
          ref={canvasDoubleEchelleRef}
          style={{ width: "100%", height: "420px" }}
          onMouseMove={survolerCanvasDouble}
          onMouseLeave={() => setSurvolDouble(null)}
        />
        {points.length > 0 && (
          <BoutonsExport
            obtenirElement={() => canvasDoubleEchelleRef.current}
            type="canvas"
            nomFichier="nomogramme-2d-temps-double-echelle"
          />
        )}
      </div>
    </div>
  );
}
