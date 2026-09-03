import { useEffect, useRef, useState } from "react";
import { api } from "../api.js";
import { CANAUX_RETRAIT } from "../nomogrammeAxes.js";
import { Button } from "../components/ui/button.jsx";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card.jsx";
import { Checkbox } from "../components/ui/checkbox.jsx";
import { Label } from "../components/ui/label.jsx";
import { RadioGroup, RadioGroupItem } from "../components/ui/radio-group.jsx";
import { classesChampNatif } from "../lib/utils.js";

// Export en masse, généré côté serveur (routes /api/export/*) — distinct des
// boutons Export CSV/Excel déjà présents sur chaque courbe (ceux-là
// n'exportent que les points déjà chargés dans le navigateur, adaptés à un
// affichage, pas à un vrai export en masse sur mesures_dewesoft : 10 Hz,
// 1,5 milliard de points, cf. logique_projet.md section 34). Trois onglets,
// un par type de mesure, chacun avec ses propres contraintes.
export default function Export() {
  const [type, setType] = useState("retrait");
  return (
    <div>
      <Card>
        <CardHeader>
          <CardTitle>Export de données</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <p className="text-sm text-muted-foreground">Génère un fichier côté serveur pour une période choisie.</p>
          <div className="flex max-w-[260px] flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Type de mesure</Label>
            <select value={type} onChange={(e) => setType(e.target.value)} className={classesChampNatif}>
              <option value="retrait">Retrait</option>
              <option value="hr_t">Température / Humidité / Point de rosée</option>
              <option value="teneur_eau">Teneur en eau</option>
            </select>
          </div>
        </CardContent>
      </Card>
      <div className="mt-5">
        {type === "retrait" && <ExportRetrait />}
        {type === "hr_t" && <ExportHrT />}
        {type === "teneur_eau" && <ExportTeneurEau />}
      </div>
    </div>
  );
}

function SelecteurFormat({ format, onChange, avecParquet = true }) {
  return (
    <div className="flex flex-col gap-1">
      <Label className="text-xs font-normal text-muted-foreground">Format</Label>
      <select value={format} onChange={(e) => onChange(e.target.value)} className={classesChampNatif}>
        <option value="csv">CSV</option>
        {avecParquet && <option value="parquet">Parquet</option>}
      </select>
    </div>
  );
}

function ExportRetrait() {
  const [canauxChoisis, setCanauxChoisis] = useState([...CANAUX_RETRAIT]);
  const [champ, setChamp] = useState("valeur_filtree");
  const [resolution, setResolution] = useState("heure");
  const [format, setFormat] = useState("csv");
  const [mode, setMode] = useState("direct"); // "direct" | "tache"
  const [debut, setDebut] = useState("");
  const [fin, setFin] = useState("");
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);
  const [tache, setTache] = useState(null); // {tache_id, statut, jours_traites, jours_total}
  const intervalleRef = useRef(null);

  const basculerCanal = (canal) =>
    setCanauxChoisis((c) => (c.includes(canal) ? c.filter((x) => x !== canal) : [...c, canal]));

  useEffect(() => {
    // Arrête le sondage si le composant se démonte (changement d'onglet).
    return () => clearInterval(intervalleRef.current);
  }, []);

  const sonderTache = (tacheId) => {
    intervalleRef.current = setInterval(async () => {
      try {
        const etat = await api.etatTacheRetrait(tacheId);
        setTache({ tache_id: tacheId, ...etat });
        if (etat.statut !== "en_cours") clearInterval(intervalleRef.current);
      } catch (e) {
        setErreur(e.message);
        clearInterval(intervalleRef.current);
      }
    }, 2000);
  };

  const exporter = async () => {
    setErreur(null);
    setEnCours(true);
    setTache(null);
    try {
      if (mode === "direct") {
        await api.exporterRetrait({ canaux: canauxChoisis.join(","), champ, debut, fin, resolution, format });
      } else {
        const { tache_id } = await api.demarrerTacheRetrait({
          canaux: canauxChoisis.join(","),
          champ,
          ...(debut ? { debut } : {}),
          ...(fin ? { fin } : {}),
          resolution,
          format,
        });
        setTache({ tache_id, statut: "en_cours", jours_traites: 0, jours_total: 1 });
        sonderTache(tache_id);
      }
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  const telechargerTache = async () => {
    setErreur(null);
    try {
      await api.telechargerTacheRetrait(tache.tache_id);
      // Le fichier est supprimé du serveur juste après l'envoi (rien ne doit
      // persister sur le VPS) : reflète l'état côté client sans attendre un
      // nouveau sondage, qui de toute façon s'est arrêté dès "termine".
      setTache((t) => (t ? { ...t, statut: "telecharge" } : t));
    } catch (e) {
      setErreur(e.message);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Retrait — un fichier, une colonne par canal</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <p className="text-sm text-muted-foreground">
          Les canaux retrait partagent tous la même grille d'horodatages (DeweSoft les enregistre simultanément) : le
          fichier généré a une ligne par instant mesuré, une colonne par canal choisi.
        </p>
        <div className="flex flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Canaux</Label>
          <div className="flex flex-wrap gap-3">
            {CANAUX_RETRAIT.map((canal) => (
              <Label key={canal} className="flex items-center gap-1.5 text-sm font-normal">
                <Checkbox checked={canauxChoisis.includes(canal)} onCheckedChange={() => basculerCanal(canal)} />
                {canal}
              </Label>
            ))}
          </div>
        </div>
        <div className="grid grid-cols-[repeat(auto-fit,minmax(140px,1fr))] gap-3">
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Grandeur</Label>
            <select value={champ} onChange={(e) => setChamp(e.target.value)} className={classesChampNatif}>
              <option value="valeur_filtree">Retrait filtré</option>
              <option value="valeur">Retrait brut</option>
            </select>
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Résolution</Label>
            <select value={resolution} onChange={(e) => setResolution(e.target.value)} className={classesChampNatif}>
              <option value="heure">Moyenne horaire</option>
              <option value="jour">Moyenne journalière</option>
              <option value="brut">Points bruts (10 Hz)</option>
            </select>
          </div>
          <SelecteurFormat format={format} onChange={setFormat} avecParquet={mode === "tache"} />
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">
              Début{mode === "tache" ? " (vide = tout l'historique)" : ""}
            </Label>
            <input type="date" value={debut} onChange={(e) => setDebut(e.target.value)} className={classesChampNatif} />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">
              Fin{mode === "tache" ? " (vide = maintenant)" : ""}
            </Label>
            <input type="date" value={fin} onChange={(e) => setFin(e.target.value)} className={classesChampNatif} />
          </div>
        </div>
        <div className="flex flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Livraison</Label>
          <RadioGroup
            value={mode}
            onValueChange={(v) => {
              setMode(v);
              // Parquet indisponible en direct (pas de fichier temporaire
              // sans borne sur le volume du VPS) : retombe sur CSV.
              if (v === "direct") setFormat((f) => (f === "parquet" ? "csv" : f));
            }}
            className="flex flex-row gap-4"
          >
            <Label className="flex items-center gap-1.5 text-sm font-normal">
              <RadioGroupItem value="direct" />
              Téléchargement direct
            </Label>
            <Label className="flex items-center gap-1.5 text-sm font-normal">
              <RadioGroupItem value="tache" />
              Tâche de fond (période longue, ou tout l'historique)
            </Label>
          </RadioGroup>
        </div>
        {mode === "direct" && (
          <p className="text-sm text-muted-foreground">
            Adapté à une période raisonnable — la réponse arrive au fur et à mesure, sans limite stricte, mais reste une
            requête classique (l'onglet doit rester ouvert). CSV uniquement (Parquet nécessite la tâche de fond).
          </p>
        )}
        {mode === "tache" && (
          <p className="text-sm text-muted-foreground">
            Génère le fichier en arrière-plan sur le serveur — vous pouvez fermer cet onglet et revenir plus tard
            consulter l'avancement, le téléchargement se fait une fois le fichier prêt.
          </p>
        )}
        {erreur && <p className="text-sm text-destructive">{erreur}</p>}
        <Button
          onClick={exporter}
          disabled={enCours || canauxChoisis.length === 0 || (mode === "direct" && (!debut || !fin))}
          className="self-start"
        >
          {enCours ? "..." : mode === "direct" ? "Générer l'export" : "Démarrer la tâche"}
        </Button>
        {tache && (
          <div className="text-sm">
            {tache.statut === "en_cours" && (
              <p>
                En cours — {tache.jours_traites} / {tache.jours_total} jour(s) traité(s)...
              </p>
            )}
            {tache.statut === "termine" && (
              <p className="flex items-center gap-2">
                Terminé ({tache.jours_total} jour(s)) —
                <Button variant="outline" size="sm" onClick={telechargerTache}>
                  Télécharger
                </Button>
              </p>
            )}
            {tache.statut === "telecharge" && (
              <p className="text-muted-foreground">
                Téléchargé — le fichier a été supprimé du serveur (rien ne persiste sur le VPS). Relancez une tâche pour
                l'obtenir à nouveau.
              </p>
            )}
            {tache.statut === "erreur" && <p className="text-destructive">Échec : {tache.erreur}</p>}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function ExportHrT() {
  const [combinaisons, setCombinaisons] = useState([]);
  const [mur, setMur] = useState("");
  const [couche, setCouche] = useState("");
  const [champsChoisis, setChampsChoisis] = useState(["temperature", "humidite", "point_de_rosee"]);
  const [format, setFormat] = useState("csv");
  const [debut, setDebut] = useState("");
  const [fin, setFin] = useState("");
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);

  useEffect(() => {
    api
      .mesuresValeursTags({ type: "hr_t" })
      .then((r) => setCombinaisons(r.combinaisons))
      .catch(() => setCombinaisons([]));
  }, []);

  const murs = [...new Set(combinaisons.map((c) => c.nom_mur).filter(Boolean))];
  const couches = [...new Set(combinaisons.map((c) => c.nom_couche).filter(Boolean))];

  const basculerChamp = (champ) =>
    setChampsChoisis((c) => (c.includes(champ) ? c.filter((x) => x !== champ) : [...c, champ]));

  const exporter = async () => {
    setErreur(null);
    setEnCours(true);
    try {
      await api.exporterHrT({
        ...(mur ? { mur } : {}),
        ...(couche ? { couche } : {}),
        champs: champsChoisis.join(","),
        debut,
        fin,
        format,
      });
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Température / Humidité / Point de rosée — un fichier, une ligne par capteur/instant</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <p className="text-sm text-muted-foreground">
          Les capteurs BLE ne partagent pas d'horodatage commun (chacun logue à son propre rythme) — contrairement au
          retrait, le fichier reste au format long (une colonne "capteur" identifie chaque ligne).
        </p>
        <div className="flex flex-col gap-1">
          <Label className="text-xs font-normal text-muted-foreground">Grandeurs</Label>
          <div className="flex flex-wrap gap-3">
            {[
              { valeur: "temperature", label: "Température" },
              { valeur: "humidite", label: "Humidité" },
              { valeur: "point_de_rosee", label: "Point de rosée" },
            ].map(({ valeur, label }) => (
              <Label key={valeur} className="flex items-center gap-1.5 text-sm font-normal">
                <Checkbox checked={champsChoisis.includes(valeur)} onCheckedChange={() => basculerChamp(valeur)} />
                {label}
              </Label>
            ))}
          </div>
        </div>
        <div className="grid grid-cols-[repeat(auto-fit,minmax(140px,1fr))] gap-3">
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Mur</Label>
            <select value={mur} onChange={(e) => setMur(e.target.value)} className={classesChampNatif}>
              <option value="">— tous —</option>
              {murs.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Couche</Label>
            <select value={couche} onChange={(e) => setCouche(e.target.value)} className={classesChampNatif}>
              <option value="">— toutes —</option>
              {couches.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </div>
          <SelecteurFormat format={format} onChange={setFormat} />
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Début</Label>
            <input type="date" value={debut} onChange={(e) => setDebut(e.target.value)} className={classesChampNatif} />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Fin</Label>
            <input type="date" value={fin} onChange={(e) => setFin(e.target.value)} className={classesChampNatif} />
          </div>
        </div>
        {erreur && <p className="text-sm text-destructive">{erreur}</p>}
        <Button
          onClick={exporter}
          disabled={enCours || champsChoisis.length === 0 || !debut || !fin}
          className="self-start"
        >
          {enCours ? "Génération..." : "Générer l'export"}
        </Button>
      </CardContent>
    </Card>
  );
}

function ExportTeneurEau() {
  const [format, setFormat] = useState("csv");
  const [debut, setDebut] = useState("");
  const [fin, setFin] = useState("");
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);

  const exporter = async () => {
    setErreur(null);
    setEnCours(true);
    try {
      await api.exporterTeneurEau({ ...(debut ? { debut } : {}), ...(fin ? { fin } : {}), format });
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Teneur en eau — toutes les saisies</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <p className="text-sm text-muted-foreground">
          Relevés ponctuels, volume négligeable — laisser Début/Fin vides pour tout exporter.
        </p>
        <div className="grid grid-cols-[repeat(auto-fit,minmax(140px,1fr))] gap-3">
          <SelecteurFormat format={format} onChange={setFormat} />
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Début</Label>
            <input type="date" value={debut} onChange={(e) => setDebut(e.target.value)} className={classesChampNatif} />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Fin</Label>
            <input type="date" value={fin} onChange={(e) => setFin(e.target.value)} className={classesChampNatif} />
          </div>
        </div>
        {erreur && <p className="text-sm text-destructive">{erreur}</p>}
        <Button onClick={exporter} disabled={enCours} className="self-start">
          {enCours ? "Génération..." : "Générer l'export"}
        </Button>
      </CardContent>
    </Card>
  );
}
