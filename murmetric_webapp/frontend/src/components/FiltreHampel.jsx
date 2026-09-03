import { useRef, useState } from "react";
import { api } from "../api.js";
import { CANAUX_RETRAIT } from "../nomogrammeAxes.js";
import BoutonsExport from "./BoutonsExport.jsx";
import { Button } from "./ui/button.jsx";
import { Input } from "./ui/input.jsx";
import { Label } from "./ui/label.jsx";
import { classesChampNatif } from "../lib/utils.js";

// Filtre de Hampel recalculé à la volée sur le retrait BRUT — répond à une
// question explicite de l'utilisateur (13/08/2026) : le seuil d'ingestion
// (HAMPEL_SEUIL_K, fixe côté ingestion_dewesoft_dxd.py) n'est pas ajustable
// depuis l'interface. Ici on ne touche à rien en base — juste un recalcul
// pour comparer visuellement différents réglages sur une courte période
// (max 2h côté backend : mesures_dewesoft est à 100 Hz, un filtre point par
// point ne peut pas s'appuyer sur une agrégation comme les autres vues).
function heureLocaleDatetimeInput(date) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

const MAINTENANT = new Date();
const IL_Y_A_1H = new Date(MAINTENANT.getTime() - 60 * 60 * 1000);

export default function FiltreHampel({ mur }) {
  const [canal, setCanal] = useState("HA1");
  const [debut, setDebut] = useState(heureLocaleDatetimeInput(IL_Y_A_1H));
  const [fin, setFin] = useState(heureLocaleDatetimeInput(MAINTENANT));
  const [fenetre, setFenetre] = useState(10);
  const [seuilK, setSeuilK] = useState(8);
  const [borneMin, setBorneMin] = useState("");
  const [borneMax, setBorneMax] = useState("");
  const [resultat, setResultat] = useState(null);
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const appliquer = async () => {
    setEnCours(true);
    setErreur(null);
    try {
      const r = await api.hampel({
        mur,
        canal_nom: canal,
        debut: new Date(debut).toISOString(),
        fin: new Date(fin).toISOString(),
        fenetre,
        seuil_k: seuilK,
        ...(borneMin !== "" && borneMax !== "" ? { borne_min: borneMin, borne_max: borneMax } : {}),
      });
      setResultat(r);
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  const points = resultat?.points ?? [];

  return (
    <div>
      <p className="text-sm text-muted-foreground">
        Recalcule le filtre de Hampel à la volée sur les valeurs brutes (rien n'est modifié en base) — pour comparer
        différents réglages de fenêtre/seuil sans toucher au réglage d'ingestion. Période limitée à 2h (données à 100
        Hz).
      </p>
      <div className="mb-3 flex flex-wrap gap-3">
        <div className="flex w-[90px] flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Canal</Label>
          <select value={canal} onChange={(e) => setCanal(e.target.value)} className={classesChampNatif}>
            {CANAUX_RETRAIT.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </div>
        <div className="flex w-[195px] flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Début</Label>
          <Input type="datetime-local" value={debut} onChange={(e) => setDebut(e.target.value)} />
        </div>
        <div className="flex w-[195px] flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Fin</Label>
          <Input type="datetime-local" value={fin} onChange={(e) => setFin(e.target.value)} />
        </div>
        <div className="flex w-[110px] flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Fenêtre</Label>
          <Input type="number" min="1" max="200" value={fenetre} onChange={(e) => setFenetre(Number(e.target.value))} />
        </div>
        <div className="flex w-[90px] flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Seuil K</Label>
          <Input
            type="number"
            min="0.1"
            step="0.1"
            value={seuilK}
            onChange={(e) => setSeuilK(Number(e.target.value))}
          />
        </div>
        <div className="flex w-[110px] flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Borne min</Label>
          <Input
            type="number"
            step="0.1"
            value={borneMin}
            onChange={(e) => setBorneMin(e.target.value)}
            placeholder="ex. -50"
          />
        </div>
        <div className="flex w-[110px] flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Borne max</Label>
          <Input
            type="number"
            step="0.1"
            value={borneMax}
            onChange={(e) => setBorneMax(e.target.value)}
            placeholder="ex. 50"
          />
        </div>
      </div>
      <p className="-mt-1 mb-3 text-xs text-muted-foreground">
        Les bornes physiques sont une 2e couche indépendante du Hampel — elles rattrapent les rafales d'échantillons
        aberrants trop longues pour la fenêtre glissante (remplacement par interpolation entre voisins valides).
      </p>
      <Button onClick={appliquer} disabled={enCours}>
        {enCours ? "Calcul..." : "Appliquer"}
      </Button>
      {erreur && <p className="text-sm text-destructive">{erreur}</p>}
      {resultat && (
        <>
          <p className="mt-3 text-sm">
            {resultat.nb_points} points ·{" "}
            <span className="text-destructive">{resultat.nb_aberrants} détecté(s) comme aberrant(s)</span> avec fenêtre=
            {resultat.fenetre}, K={resultat.seuil_k}
          </p>
          {points.length > 0 && <GraphiqueHampel points={points} />}
        </>
      )}
    </div>
  );
}

function GraphiqueHampel({ points }) {
  const svgRef = useRef(null);
  const largeur = 900,
    hauteur = 320,
    marge = 40;
  const temps = points.map((p) => new Date(p.time).getTime());
  const brut = points.map((p) => p.brut);
  const filtre = points.map((p) => p.filtre_ajuste);
  const tMin = Math.min(...temps),
    tMax = Math.max(...temps);
  const vMin = Math.min(...brut, ...filtre),
    vMax = Math.max(...brut, ...filtre);

  const x = (t) => marge + ((t - tMin) / (tMax - tMin || 1)) * (largeur - 2 * marge);
  const y = (v) => hauteur - marge - ((v - vMin) / (vMax - vMin || 1)) * (hauteur - 2 * marge);

  const cheminBrut = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(temps[i])},${y(p.brut)}`).join(" ");
  const cheminFiltre = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(temps[i])},${y(p.filtre_ajuste)}`).join(" ");

  return (
    <div>
      <svg ref={svgRef} viewBox={`0 0 ${largeur} ${hauteur}`} width="100%" height={hauteur}>
        <path d={cheminBrut} fill="none" stroke="#6b7280" strokeWidth="1" />
        <path d={cheminFiltre} fill="none" stroke="var(--ring)" strokeWidth="1.5" />
        {points.map(
          (p, i) => p.aberrant && <circle key={i} cx={x(temps[i])} cy={y(p.brut)} r="3" fill="var(--destructive)" />,
        )}
        <text x={marge} y={16} fill="#6b7280" fontSize="12">
          — brut
        </text>
        <text x={marge + 60} y={16} fill="var(--ring)" fontSize="12">
          — filtré (ajusté)
        </text>
        <text x={marge + 190} y={16} fill="var(--destructive)" fontSize="12">
          ● point corrigé
        </text>
      </svg>
      <BoutonsExport obtenirElement={() => svgRef.current} type="svg" nomFichier="filtre-hampel" />
    </div>
  );
}
