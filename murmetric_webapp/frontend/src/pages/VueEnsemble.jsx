import { useState } from "react";
import { api } from "../api.js";
import FiltreHampel from "../components/FiltreHampel.jsx";
import GraphiqueSVG from "../components/GraphiqueSVG.jsx";
import Nomogramme from "../components/Nomogramme.jsx";
import Nomogramme3D from "../components/Nomogramme3D.jsx";
import SelecteurMesure from "../components/SelecteurMesure.jsx";
import { Button } from "../components/ui/button.jsx";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card.jsx";
import { Label } from "../components/ui/label.jsx";
import { useEtatVueEnsemble } from "../EtatPagesContext.jsx";
import { classesChampNatif } from "../lib/utils.js";
import { TYPES_TRACE, libelleGrandeur } from "../nomogrammeAxes.js";

export default function VueEnsemble() {
  // Sélection/courbe/mode 3D/type de tracé préservés en changeant d'onglet
  // (EtatPagesContext) — erreur/chargement restent locaux, purement
  // transitoires.
  const { selection, setSelection, points, setPoints, mode3D, setMode3D, typeTrace, setTypeTrace } =
    useEtatVueEnsemble();
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const charger = async () => {
    setEnCours(true);
    setErreur(null);
    try {
      const params = Object.fromEntries(Object.entries(selection).filter(([, v]) => v));
      const resultat = await api.mesures(params);
      setPoints(resultat.points);
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Vue d'ensemble</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <SelecteurMesure valeur={selection} onChange={setSelection}>
            <div className="flex flex-col gap-1">
              <Label className="text-xs font-normal text-muted-foreground">Type de tracé</Label>
              <select value={typeTrace} onChange={(e) => setTypeTrace(e.target.value)} className={classesChampNatif}>
                {TYPES_TRACE.map((t) => (
                  <option key={t.valeur} value={t.valeur}>
                    {t.label}
                  </option>
                ))}
              </select>
            </div>
          </SelecteurMesure>
          <Button onClick={charger} disabled={enCours} className="self-start">
            {enCours ? "Chargement..." : "Charger la courbe temporelle"}
          </Button>
          {erreur && <p className="text-sm text-destructive">{erreur}</p>}
        </CardContent>
      </Card>
      {points && points.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Courbe — {libelleGrandeur(`${selection.type}:${selection.champ}`)}</CardTitle>
          </CardHeader>
          <CardContent>
            <GraphiqueSVG points={points} champ={selection.champ} typeTrace={typeTrace} />
          </CardContent>
        </Card>
      )}
      {points && points.length === 0 && (
        <Card>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Aucune donnée pour cette sélection sur la période choisie — essaie d'élargir la période, ou vérifie le
              mur/la couche (la dernière mesure disponible peut être plus ancienne que la période demandée).
            </p>
          </CardContent>
        </Card>
      )}
      {(selection.type === "hr_t" || selection.type === "retrait") && (
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle>Nomogramme — grandeurs croisées</CardTitle>
              <div className="flex gap-2">
                <Button
                  variant={!mode3D ? "default" : "outline"}
                  size="sm"
                  onClick={() => setMode3D(false)}
                  disabled={!mode3D}
                >
                  2D
                </Button>
                <Button
                  variant={mode3D ? "default" : "outline"}
                  size="sm"
                  onClick={() => setMode3D(true)}
                  disabled={mode3D}
                >
                  3D (HR/T + retrait)
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              {mode3D
                ? "Compose librement 3 grandeurs (HR/T, retrait, temps) — glisse pour tourner, molette pour zoomer, survole un point pour lire ses valeurs."
                : "Compose librement 2 grandeurs (HR/T, retrait, temps) — survole un point pour lire sa valeur par projection sur les axes."}
            </p>
            {mode3D ? (
              <Nomogramme3D
                mur={selection.mur}
                couche={selection.couche}
                debutInitial={selection.debut}
                finInitial={selection.fin}
              />
            ) : (
              <Nomogramme
                mur={selection.mur}
                couche={selection.couche}
                debutInitial={selection.debut}
                finInitial={selection.fin}
              />
            )}
          </CardContent>
        </Card>
      )}
      {selection.type === "retrait" && (
        <Card>
          <CardHeader>
            <CardTitle>Filtre de Hampel — comparer brut/filtré avec un réglage ajustable</CardTitle>
          </CardHeader>
          <CardContent>
            <FiltreHampel mur={selection.mur} />
          </CardContent>
        </Card>
      )}
    </div>
  );
}
