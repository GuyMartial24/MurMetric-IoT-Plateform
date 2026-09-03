import { useEffect, useState } from "react";
import { api } from "../api.js";
import BoutonsExportDonnees from "../components/BoutonsExportDonnees.jsx";
import ChampSelectOuAutre from "../components/ChampSelectOuAutre.jsx";
import { Button } from "../components/ui/button.jsx";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card.jsx";
import { Input } from "../components/ui/input.jsx";
import { Label } from "../components/ui/label.jsx";
import { Skeleton } from "../components/ui/skeleton.jsx";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "../components/ui/table.jsx";
import { classesChampNatif } from "../lib/utils.js";
import { useCouchesParMur, useMursCouchesConnus } from "../mursCouches.js";

const VIDE = { mur: "", couche: "", valeur_pourcent: "", commentaire: "", date_mesure: "" };

export default function TeneurEau() {
  const [liste, setListe] = useState([]);
  const [chargementInitial, setChargementInitial] = useState(true);
  const [saisie, setSaisie] = useState(VIDE);
  const [enEdition, setEnEdition] = useState(null); // { original, valeurs }
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);
  const [filtreMur, setFiltreMur] = useState("");
  const [filtreCouche, setFiltreCouche] = useState("");
  const { murs: mursConnus } = useMursCouchesConnus();
  // Couches filtrées par mur + type teneur_eau (31/08/2026, demande
  // explicite) — 2 listes indépendantes, la saisie et l'édition en ligne ne
  // portant pas forcément sur le même mur au même instant.
  const couchesPourSaisie = useCouchesParMur("teneur_eau", saisie.mur);
  const couchesPourEdition = useCouchesParMur("teneur_eau", enEdition?.valeurs?.mur);

  const charger = async () => {
    try {
      setListe(await api.listerTeneurEau());
    } catch (e) {
      setErreur(e.message);
    } finally {
      setChargementInitial(false);
    }
  };

  useEffect(() => {
    charger();
  }, []);

  const soumettre = async (e) => {
    e.preventDefault();
    setEnCours(true);
    setErreur(null);
    try {
      await api.creerTeneurEau({
        ...saisie,
        valeur_pourcent: Number(saisie.valeur_pourcent),
        date_mesure: saisie.date_mesure ? new Date(saisie.date_mesure).toISOString() : null,
      });
      setSaisie(VIDE);
      await charger();
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  const demarrerEdition = (ligne) => {
    setEnEdition({
      original: { mur: ligne.mur, couche: ligne.couche, date_mesure: ligne.time },
      valeurs: {
        mur: ligne.mur,
        couche: ligne.couche,
        valeur_pourcent: ligne.value,
        commentaire: ligne.commentaire || "",
        date_mesure: ligne.time,
      },
    });
  };

  const enregistrerCorrection = async () => {
    setErreur(null);
    try {
      await api.corrigerTeneurEau({
        mur_original: enEdition.original.mur,
        couche_original: enEdition.original.couche,
        date_mesure_original: enEdition.original.date_mesure,
        mur: enEdition.valeurs.mur,
        couche: enEdition.valeurs.couche,
        valeur_pourcent: Number(enEdition.valeurs.valeur_pourcent),
        commentaire: enEdition.valeurs.commentaire,
        date_mesure: enEdition.valeurs.date_mesure,
      });
      setEnEdition(null);
      await charger();
    } catch (e) {
      setErreur(e.message);
    }
  };

  // Une entrée = 2 lignes brutes InfluxDB (field teneur_eau_pourcent +
  // field commentaire, même mesure/tags/heure) — regroupées ici par clé
  // (mur, couche, time) pour l'affichage.
  const groupes = Object.values(
    liste.reduce((acc, p) => {
      const cle = `${p.mur}|${p.couche}|${p.time}`;
      acc[cle] = acc[cle] || { mur: p.mur, couche: p.couche, time: p.time };
      if (p.field === "teneur_eau_pourcent") acc[cle].value = p.value;
      if (p.field === "commentaire") acc[cle].commentaire = p.value;
      return acc;
    }, {}),
  );

  // Filtres Mur/Couche (27/08/2026) — indépendants l'un de l'autre, menus
  // dérivés des valeurs déjà présentes plutôt qu'une saisie libre.
  const mursDisponibles = [...new Set(groupes.map((g) => g.mur))].sort((a, b) => a.localeCompare(b));
  const couchesDisponibles = [...new Set(groupes.map((g) => g.couche))].sort((a, b) => a.localeCompare(b));
  const groupesFiltres = groupes.filter(
    (g) => (!filtreMur || g.mur === filtreMur) && (!filtreCouche || g.couche === filtreCouche),
  );

  return (
    <div className="flex flex-col gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Nouvelle saisie — teneur en eau</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <form onSubmit={soumettre} className="grid grid-cols-[repeat(auto-fit,minmax(140px,1fr))] gap-3">
            <div className="flex flex-col gap-1">
              <Label className="text-xs font-normal text-muted-foreground">Mur</Label>
              <ChampSelectOuAutre
                required
                valeur={saisie.mur}
                options={mursConnus}
                onChange={(v) => setSaisie({ ...saisie, mur: v })}
                placeholder="SOCMA 1"
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label className="text-xs font-normal text-muted-foreground">Couche</Label>
              <ChampSelectOuAutre
                required
                valeur={saisie.couche}
                options={couchesPourSaisie}
                onChange={(v) => setSaisie({ ...saisie, couche: v })}
                placeholder="interface carreau et exterieur"
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label className="text-xs font-normal text-muted-foreground">Valeur (%)</Label>
              <Input
                required
                type="number"
                step="0.01"
                value={saisie.valeur_pourcent}
                onChange={(e) => setSaisie({ ...saisie, valeur_pourcent: e.target.value })}
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label className="text-xs font-normal text-muted-foreground">Date de mesure</Label>
              <Input
                type="datetime-local"
                value={saisie.date_mesure}
                onChange={(e) => setSaisie({ ...saisie, date_mesure: e.target.value })}
              />
            </div>
            <div className="col-span-full flex flex-col gap-1">
              <Label className="text-xs font-normal text-muted-foreground">Commentaire</Label>
              <Input
                value={saisie.commentaire}
                onChange={(e) => setSaisie({ ...saisie, commentaire: e.target.value })}
              />
            </div>
          </form>
          <Button onClick={soumettre} disabled={enCours} className="self-start">
            {enCours ? "Envoi..." : "Enregistrer"}
          </Button>
          {erreur && <p className="text-sm text-destructive">{erreur}</p>}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>
            Saisies existantes ({groupesFiltres.length}
            {groupesFiltres.length !== groupes.length ? ` / ${groupes.length}` : ""})
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <BoutonsExportDonnees lignes={groupesFiltres} nomFichier="teneur_eau" />
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Mur</TableHead>
                <TableHead>Couche</TableHead>
                <TableHead>Valeur (%)</TableHead>
                <TableHead>Date</TableHead>
                <TableHead>Commentaire</TableHead>
                <TableHead></TableHead>
              </TableRow>
              <TableRow>
                <TableHead>
                  <select
                    value={filtreMur}
                    onChange={(e) => setFiltreMur(e.target.value)}
                    className={classesChampNatif}
                  >
                    <option value="">Tous</option>
                    {mursDisponibles.map((m) => (
                      <option key={m} value={m}>
                        {m}
                      </option>
                    ))}
                  </select>
                </TableHead>
                <TableHead>
                  <select
                    value={filtreCouche}
                    onChange={(e) => setFiltreCouche(e.target.value)}
                    className={classesChampNatif}
                  >
                    <option value="">Toutes</option>
                    {couchesDisponibles.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                </TableHead>
                <TableHead></TableHead>
                <TableHead></TableHead>
                <TableHead></TableHead>
                <TableHead></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {chargementInitial
                ? Array.from({ length: 5 }, (_, i) => (
                    <TableRow key={`squelette-${i}`}>
                      {Array.from({ length: 6 }, (_, j) => (
                        <TableCell key={j}>
                          <Skeleton className="h-4 w-full" />
                        </TableCell>
                      ))}
                    </TableRow>
                  ))
                : groupesFiltres.map((g) => (
                    <TableRow key={`${g.mur}|${g.couche}|${g.time}`}>
                      {enEdition?.original.mur === g.mur &&
                      enEdition.original.couche === g.couche &&
                      enEdition.original.date_mesure === g.time ? (
                        <>
                          <TableCell className="whitespace-normal">
                            <ChampSelectOuAutre
                              valeur={enEdition.valeurs.mur}
                              options={mursConnus}
                              onChange={(v) =>
                                setEnEdition({ ...enEdition, valeurs: { ...enEdition.valeurs, mur: v } })
                              }
                            />
                          </TableCell>
                          <TableCell className="whitespace-normal">
                            <ChampSelectOuAutre
                              valeur={enEdition.valeurs.couche}
                              options={couchesPourEdition}
                              onChange={(v) =>
                                setEnEdition({ ...enEdition, valeurs: { ...enEdition.valeurs, couche: v } })
                              }
                            />
                          </TableCell>
                          <TableCell className="whitespace-normal">
                            <input
                              type="number"
                              step="0.01"
                              value={enEdition.valeurs.valeur_pourcent}
                              onChange={(e) =>
                                setEnEdition({
                                  ...enEdition,
                                  valeurs: { ...enEdition.valeurs, valeur_pourcent: e.target.value },
                                })
                              }
                              className={classesChampNatif}
                            />
                          </TableCell>
                          <TableCell colSpan={2} className="whitespace-normal">
                            <input
                              value={enEdition.valeurs.commentaire}
                              onChange={(e) =>
                                setEnEdition({
                                  ...enEdition,
                                  valeurs: { ...enEdition.valeurs, commentaire: e.target.value },
                                })
                              }
                              className={classesChampNatif}
                            />
                          </TableCell>
                          <TableCell>
                            <div className="flex gap-1.5">
                              <Button size="sm" onClick={enregistrerCorrection}>
                                Enregistrer
                              </Button>
                              <Button size="sm" variant="outline" onClick={() => setEnEdition(null)}>
                                Annuler
                              </Button>
                            </div>
                          </TableCell>
                        </>
                      ) : (
                        <>
                          <TableCell className="whitespace-normal">{g.mur}</TableCell>
                          <TableCell className="whitespace-normal">{g.couche}</TableCell>
                          <TableCell className="whitespace-normal font-mono">{g.value?.toFixed(2)}</TableCell>
                          <TableCell className="whitespace-normal">
                            {new Date(g.time).toLocaleString("fr-FR")}
                          </TableCell>
                          <TableCell className="whitespace-normal">{g.commentaire}</TableCell>
                          <TableCell>
                            <Button size="sm" variant="outline" onClick={() => demarrerEdition(g)}>
                              Éditer
                            </Button>
                          </TableCell>
                        </>
                      )}
                    </TableRow>
                  ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
