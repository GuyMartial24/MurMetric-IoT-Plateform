import { useEffect, useState } from "react";
import { api } from "../api.js";
import BoutonsExportDonnees from "../components/BoutonsExportDonnees.jsx";
import ChampSelectOuAutre from "../components/ChampSelectOuAutre.jsx";
import Pastille from "../components/Pastille.jsx";
import { Button } from "../components/ui/button.jsx";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card.jsx";
import { Checkbox } from "../components/ui/checkbox.jsx";
import { Skeleton } from "../components/ui/skeleton.jsx";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "../components/ui/table.jsx";
import { classesChampNatif } from "../lib/utils.js";
import { useCouchesParMur, useMursCouchesConnus } from "../mursCouches.js";

// Édition en place (chantier "source unique", section 32, 13/08/2026) :
// capteurs.json/capteurs_retrait.json vivent désormais sur le volume
// persistant de la webapp, qui en est la source de vérité — le PC Amiens et
// le Pi interrogent son API au lieu de leur copie locale, donc une
// modification faite ici a un effet réel sur l'étiquetage des prochaines
// mesures (pas seulement cosmétique comme avant ce chantier).
const INTERVALLE_RAFRAICHISSEMENT_MS = 30_000; // même cadence que Monitoring.jsx

export default function Capteurs() {
  const [hrT, setHrT] = useState(null);
  const [retrait, setRetrait] = useState(null);
  const [erreur, setErreur] = useState(null);

  const charger = () =>
    Promise.all([api.capteursHrT(), api.capteursRetrait(), api.dernieresMesuresHrT()])
      .then(([h, r, mesures]) => {
        setHrT(
          Object.fromEntries(
            Object.entries(h).map(([cle, c]) =>
              cle === "_schema" ? [cle, c] : [cle, { ...c, derniere_mesure: mesures[cle] }],
            ),
          ),
        );
        setRetrait(r);
      })
      .catch((e) => setErreur(e.message));

  useEffect(() => {
    charger();
    const id = setInterval(charger, INTERVALLE_RAFRAICHISSEMENT_MS);
    return () => clearInterval(id);
  }, []);

  const lignes = (donnees) => (donnees ? Object.entries(donnees).filter(([cle]) => cle !== "_schema") : []);

  return (
    <div className="flex flex-col gap-5">
      {erreur && <p className="text-sm text-destructive">{erreur}</p>}

      <TableauCapteurs
        titre="Capteurs HR/T"
        typeMesure="hr_t"
        lignes={lignes(hrT)}
        chargement={hrT === null}
        colonnes={[
          "nom",
          "famille_capteur",
          "nom_mur",
          "nom_couche",
          "ingestion",
          "derniere_detection",
          "dernier_rssi",
          "derniere_batterie",
          "derniere_mesure",
          "lint_cible_s",
        ]}
        champsEditables={["nom", "nom_mur", "nom_couche", "ingestion", "lint_cible_s", "note_frequence_nfc"]}
        colonnesFiltrables={["famille_capteur", "nom_mur", "nom_couche", "ingestion"]}
        cleColonne="MAC / clé"
        enregistrer={(cle, champs) => api.modifierCapteurHrT(cle, champs)}
        recharger={charger}
        nomFichierExport="capteurs_hr_t"
      />

      <TableauCapteurs
        titre="Canaux retrait"
        typeMesure="retrait"
        lignes={lignes(retrait)}
        chargement={retrait === null}
        colonnes={["nom_mur", "nom_couche", "position", "ingestion"]}
        champsEditables={["nom_mur", "nom_couche", "position", "ingestion"]}
        colonnesFiltrables={["nom_mur", "nom_couche", "position", "ingestion"]}
        cleColonne="Canal"
        enregistrer={(cle, champs) => api.modifierCapteurRetrait(cle, champs)}
        recharger={charger}
        nomFichierExport="capteurs_retrait"
        avertissementEdition={
          "Cette modification ne s'appliquera qu'aux nouvelles mesures — " +
          "l'historique déjà enregistré gardera l'ancien étiquetage. Pour " +
          "retrouver l'historique complet d'un canal malgré un renommage, " +
          "utilisez le sélecteur « Canal » plutôt que Mur/Couche."
        }
      />
    </div>
  );
}

const LIBELLES = {
  nom: "Nom",
  famille_capteur: "Famille",
  nom_mur: "Mur",
  nom_couche: "Couche",
  position: "Position",
  ingestion: "Ingestion",
  derniere_detection: "Dernière détection",
  dernier_rssi: "RSSI (dBm)",
  derniere_batterie: "Batterie",
  derniere_mesure: "Dernière mesure",
  lint_cible_s: "Intervalle mesure",
};

// Intervalle de mesure Blue Maestro (26/08/2026) — réglable par capteur,
// GATT via configure_capteurs.py sur le Pi (cf. logique_projet.md). Non
// applicable à ELA : configuration NFC uniquement, aucune commande à
// distance possible (encore moins une fois le capteur noyé dans le mur).
function formatDuree(s) {
  if (s == null) return "?";
  if (s % 86400 === 0) return `${s / 86400} j`;
  if (s % 3600 === 0) return `${s / 3600} h`;
  if (s % 60 === 0) return `${s / 60} min`;
  return `${s} s`;
}

// Valeurs proposées pour lint_cible_s (26/08/2026) — menu déroulant plutôt
// qu'un champ en secondes brutes, plus lisible et évite les erreurs de
// saisie (ex. "864000" au lieu de "86400"). Bornes matérielles Blue
// Maestro : 1 s - 86 400 s (24 h), cf. configure_capteurs.py.
const PRESETS_LINT_CIBLE_S = [
  { s: 60, libelle: "1 min" },
  { s: 120, libelle: "2 min" },
  { s: 180, libelle: "3 min" },
  { s: 240, libelle: "4 min" },
  { s: 300, libelle: "5 min" },
  { s: 900, libelle: "15 min" },
  { s: 1800, libelle: "30 min" },
  { s: 3600, libelle: "1 h" },
  { s: 7200, libelle: "2 h" },
  { s: 10800, libelle: "3 h" },
  { s: 14400, libelle: "4 h" },
  { s: 21600, libelle: "6 h" },
  { s: 28800, libelle: "8 h" },
  { s: 43200, libelle: "12 h" },
  { s: 86400, libelle: "24 h" },
];

// Télémétrie (dernière détection/RSSI/batterie, 19/08/2026) : envoyée par le
// script d'ingestion du Pi indépendamment du flag ingestion (utile pour
// surveiller un capteur pas encore activé, ou anticiper une perte de signal
// — capteurs noyés dans les parois, impossibles à vérifier physiquement).
// Seuils calés sur l'intervalle d'envoi throttlé côté Pi (5 min) : un
// capteur sain devrait quasi-systématiquement apparaître "récent".
function etatDerniereDetection(iso) {
  if (!iso) return { texte: "Jamais vu", etat: "neutre" };
  const minutes = (Date.now() - new Date(iso).getTime()) / 60000;
  const texte =
    minutes < 60
      ? `il y a ${Math.round(minutes)} min`
      : minutes < 1440
        ? `il y a ${Math.round(minutes / 60)} h`
        : `il y a ${Math.round(minutes / 1440)} j`;
  const etat = minutes < 15 ? "ok" : minutes < 180 ? "attention" : "erreur";
  return { texte, etat };
}

// Blue Maestro transmet un pourcentage en continu (absent = jamais reçu).
// ELA (mode Service Data, 19/08/2026) ne transmet CE champ que lorsque sa
// batterie réelle est déjà sous 15% (cf. logique_projet.md section 40,
// addendum) — donc pour ce capteur, "absent" ne veut pas dire "jamais reçu"
// mais "rien à signaler", à condition d'avoir déjà reçu au moins une
// détection (sinon on ne sait vraiment rien, cf. dernier paramètre).
function etatBatterie(pourcentage, familleCapteur, derniereDetection) {
  if (pourcentage !== undefined && pourcentage !== null) {
    const etat = pourcentage < 15 ? "erreur" : pourcentage < 30 ? "attention" : "ok";
    return { texte: `${pourcentage} %`, etat };
  }
  if (familleCapteur === "ela" && derniereDetection) {
    return { texte: "Saine (> 15 %)", etat: "ok" };
  }
  return null;
}

// Dernière mesure réellement écrite dans InfluxDB (température/humidité,
// 26/08/2026) — distincte de la télémétrie détection/RSSI/batterie
// ci-dessus, qui ne prouve que la réception du signal BLE, pas qu'une
// mesure ait été publiée (n'existe que pour les capteurs ingestion: true,
// cf. dernieres_mesures_hr_t côté backend). Mêmes seuils de fraîcheur que
// etatDerniereDetection. Date affichée seulement si différente d'aujourd'hui
// (question utilisateur : sans ça, rien ne distingue "aujourd'hui 19:26"
// d'un horodatage vieux de plusieurs jours à la même heure).
function texteMesure(mesure) {
  if (!mesure || mesure.heure == null) return null;
  const parties = [];
  if (mesure.temperature != null) parties.push(`${mesure.temperature.toFixed(1)}°C`);
  if (mesure.humidite != null) parties.push(`${mesure.humidite.toFixed(0)} %`);
  const date = new Date(mesure.heure);
  const aujourdhui = date.toDateString() === new Date().toDateString();
  const heure = aujourdhui
    ? date.toLocaleTimeString("fr-FR")
    : `${date.toLocaleDateString("fr-FR", { day: "2-digit", month: "2-digit" })} ${date.toLocaleTimeString("fr-FR")}`;
  const minutes = (Date.now() - date.getTime()) / 60000;
  const etat = minutes < 15 ? "ok" : minutes < 180 ? "attention" : "erreur";
  return { texte: `${parties.join(" · ")} — ${heure}`, etat };
}

// Valeur d'un champ telle qu'utilisée pour le filtrage (27/08/2026) —
// distincte du rendu visuel (Pastille, etc.) : juste une chaîne comparable,
// notamment pour "ingestion" (booléen) affiché "Oui"/"Non" dans le menu.
function valeurPourFiltre(champ, c) {
  if (champ === "ingestion") return c.ingestion ? "Oui" : "Non";
  return c[champ] ?? "";
}

function TableauCapteurs({
  titre,
  typeMesure,
  lignes,
  colonnes,
  champsEditables,
  colonnesFiltrables = [],
  cleColonne,
  enregistrer,
  recharger,
  nomFichierExport,
  avertissementEdition,
  chargement = false,
}) {
  const [enEdition, setEnEdition] = useState(null); // { cle, valeurs }
  const [enCours, setEnCours] = useState(false);
  const [erreur, setErreur] = useState(null);
  const [filtres, setFiltres] = useState({}); // { _cle: "texte", [champ]: "valeur" }
  const { murs: mursConnus } = useMursCouchesConnus();
  // Couches filtrées par mur + type (31/08/2026, demande explicite) — sans
  // ça, éditer une couche ici proposait un mélange de couches HR/T, retrait
  // ET teneur en eau, la plupart sans rapport avec la ligne en cours
  // d'édition. `enEdition?.valeurs?.nom_mur` reste undefined hors édition,
  // le hook renvoie alors simplement [] (inoffensif, le champ n'est rendu
  // qu'en édition).
  const couchesConnues = useCouchesParMur(typeMesure, enEdition?.valeurs?.nom_mur);

  // Filtres indépendants les uns des autres (pas de filtrage en cascade,
  // plus simple à comprendre) — menu déroulant pour les colonnes
  // catégorielles (colonnesFiltrables), recherche texte pour la clé et pour
  // "nom" quand elle est présente dans les colonnes affichées.
  const lignesFiltrees = lignes.filter(([cle, c]) => {
    if (filtres._cle && !cle.toLowerCase().includes(filtres._cle.toLowerCase())) return false;
    for (const champ of colonnes) {
      const valeurFiltre = filtres[champ];
      if (!valeurFiltre) continue;
      if (champ === "nom") {
        if (
          !String(c.nom ?? "")
            .toLowerCase()
            .includes(valeurFiltre.toLowerCase())
        )
          return false;
      } else if (colonnesFiltrables.includes(champ)) {
        if (valeurPourFiltre(champ, c) !== valeurFiltre) return false;
      }
    }
    return true;
  });

  const optionsPourColonne = (champ) =>
    [...new Set(lignes.map(([, c]) => valeurPourFiltre(champ, c)))]
      .filter((v) => v !== "")
      .sort((a, b) => a.localeCompare(b));

  const demarrerEdition = (cle, c) => {
    setErreur(null);
    setEnEdition({
      cle,
      valeurs: Object.fromEntries(
        champsEditables.map((champ) => [
          champ,
          c[champ] ?? (champ === "ingestion" ? false : champ === "lint_cible_s" ? 86400 : ""),
        ]),
      ),
    });
  };

  const valider = async () => {
    setEnCours(true);
    setErreur(null);
    try {
      await enregistrer(enEdition.cle, enEdition.valeurs);
      setEnEdition(null);
      await recharger();
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>
          {titre} ({lignesFiltrees.length}
          {lignesFiltrees.length !== lignes.length ? ` / ${lignes.length}` : ""})
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <BoutonsExportDonnees
          lignes={lignesFiltrees.map(([cle, c]) => ({ [cleColonne]: cle, ...c }))}
          nomFichier={nomFichierExport}
        />
        {erreur && <p className="text-sm text-destructive">{erreur}</p>}
        {enEdition && avertissementEdition && <p className="text-sm text-warning">{avertissementEdition}</p>}
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{cleColonne}</TableHead>
              {colonnes.map((champ) => (
                <TableHead key={champ}>{LIBELLES[champ]}</TableHead>
              ))}
              <TableHead></TableHead>
            </TableRow>
            <TableRow>
              <TableHead>
                <input
                  placeholder="Filtrer..."
                  value={filtres._cle || ""}
                  onChange={(e) => setFiltres({ ...filtres, _cle: e.target.value })}
                  className={classesChampNatif}
                />
              </TableHead>
              {colonnes.map((champ) => (
                <TableHead key={champ}>
                  {champ === "nom" ? (
                    <input
                      placeholder="Filtrer..."
                      value={filtres[champ] || ""}
                      onChange={(e) => setFiltres({ ...filtres, [champ]: e.target.value })}
                      className={classesChampNatif}
                    />
                  ) : colonnesFiltrables.includes(champ) ? (
                    <select
                      value={filtres[champ] || ""}
                      onChange={(e) => setFiltres({ ...filtres, [champ]: e.target.value })}
                      className={classesChampNatif}
                    >
                      <option value="">Tous</option>
                      {optionsPourColonne(champ).map((v) => (
                        <option key={v} value={v}>
                          {v}
                        </option>
                      ))}
                    </select>
                  ) : null}
                </TableHead>
              ))}
              <TableHead></TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {chargement
              ? Array.from({ length: 5 }, (_, i) => (
                  <TableRow key={`squelette-${i}`}>
                    <TableCell>
                      <Skeleton className="h-4 w-full" />
                    </TableCell>
                    {colonnes.map((champ) => (
                      <TableCell key={champ}>
                        <Skeleton className="h-4 w-full" />
                      </TableCell>
                    ))}
                    <TableCell>
                      <Skeleton className="h-4 w-full" />
                    </TableCell>
                  </TableRow>
                ))
              : lignesFiltrees.map(([cle, c]) => {
                  const edition = enEdition?.cle === cle;
                  return (
                    <TableRow key={cle}>
                      <TableCell>{cle}</TableCell>
                      {colonnes.map((champ) => (
                        <TableCell key={champ} className="whitespace-normal">
                          {edition && champsEditables.includes(champ) ? (
                            champ === "ingestion" ? (
                              <Checkbox
                                checked={enEdition.valeurs.ingestion}
                                onCheckedChange={(checked) =>
                                  setEnEdition({
                                    ...enEdition,
                                    valeurs: { ...enEdition.valeurs, ingestion: checked === true },
                                  })
                                }
                              />
                            ) : champ === "lint_cible_s" ? (
                              c.famille_capteur === "bluemaestro" ? (
                                <select
                                  value={enEdition.valeurs.lint_cible_s}
                                  onChange={(e) =>
                                    setEnEdition({
                                      ...enEdition,
                                      valeurs: { ...enEdition.valeurs, lint_cible_s: Number(e.target.value) },
                                    })
                                  }
                                  className={classesChampNatif}
                                >
                                  {PRESETS_LINT_CIBLE_S.map((p) => (
                                    <option key={p.s} value={p.s}>
                                      {p.libelle}
                                    </option>
                                  ))}
                                </select>
                              ) : c.famille_capteur === "ela" ? (
                                <input
                                  placeholder="Pense-bête, ex. « 5 min »"
                                  value={enEdition.valeurs.note_frequence_nfc}
                                  onChange={(e) =>
                                    setEnEdition({
                                      ...enEdition,
                                      valeurs: { ...enEdition.valeurs, note_frequence_nfc: e.target.value },
                                    })
                                  }
                                  className={classesChampNatif}
                                />
                              ) : (
                                "—"
                              )
                            ) : champ === "nom_mur" || champ === "nom_couche" ? (
                              <ChampSelectOuAutre
                                valeur={enEdition.valeurs[champ]}
                                options={champ === "nom_mur" ? mursConnus : couchesConnues}
                                onChange={(v) =>
                                  setEnEdition({ ...enEdition, valeurs: { ...enEdition.valeurs, [champ]: v } })
                                }
                              />
                            ) : (
                              <input
                                value={enEdition.valeurs[champ]}
                                onChange={(e) =>
                                  setEnEdition({
                                    ...enEdition,
                                    valeurs: { ...enEdition.valeurs, [champ]: e.target.value },
                                  })
                                }
                                className={classesChampNatif}
                              />
                            )
                          ) : champ === "ingestion" ? (
                            c.ingestion ? (
                              <Pastille etat="ok" texte="Oui" />
                            ) : (
                              "—"
                            )
                          ) : champ === "derniere_detection" ? (
                            (() => {
                              const { texte, etat } = etatDerniereDetection(c.derniere_detection);
                              return <Pastille etat={etat} texte={texte} />;
                            })()
                          ) : champ === "derniere_batterie" ? (
                            (() => {
                              const info = etatBatterie(c.derniere_batterie, c.famille_capteur, c.derniere_detection);
                              return info ? <Pastille etat={info.etat} texte={info.texte} /> : "—";
                            })()
                          ) : champ === "dernier_rssi" ? (
                            <span className="font-mono">
                              {c.dernier_rssi !== undefined && c.dernier_rssi !== null ? c.dernier_rssi : "—"}
                            </span>
                          ) : champ === "lint_cible_s" ? (
                            c.famille_capteur === "bluemaestro" ? (
                              (() => {
                                const cible = c.lint_cible_s ?? 86400;
                                const applique = c.lint_max_confirme_s;
                                return applique === cible ? (
                                  <Pastille etat="ok" texte={formatDuree(applique)} />
                                ) : (
                                  <Pastille
                                    etat="attention"
                                    texte={`${formatDuree(cible)} (cible${
                                      applique != null ? `, ${formatDuree(applique)} appliqué` : ", en attente"
                                    })`}
                                  />
                                );
                              })()
                            ) : c.famille_capteur === "ela" ? (
                              <div>
                                <div>NFC : {c.note_frequence_nfc || "—"}</div>
                                <div className="text-xs opacity-70">
                                  Observé :{" "}
                                  {c.derniere_mesure?.intervalle_observe_s != null
                                    ? formatDuree(Math.round(c.derniere_mesure.intervalle_observe_s))
                                    : "—"}
                                </div>
                              </div>
                            ) : (
                              "—"
                            )
                          ) : champ === "derniere_mesure" ? (
                            (() => {
                              const info = texteMesure(c.derniere_mesure);
                              return info ? <Pastille etat={info.etat} texte={info.texte} /> : "—";
                            })()
                          ) : (
                            c[champ]
                          )}
                        </TableCell>
                      ))}
                      <TableCell>
                        {edition ? (
                          <div className="flex gap-1.5">
                            <Button size="sm" onClick={valider} disabled={enCours}>
                              {enCours ? "..." : "Enregistrer"}
                            </Button>
                            <Button size="sm" variant="outline" onClick={() => setEnEdition(null)} disabled={enCours}>
                              Annuler
                            </Button>
                          </div>
                        ) : (
                          <Button size="sm" variant="outline" onClick={() => demarrerEdition(cle, c)}>
                            Éditer
                          </Button>
                        )}
                      </TableCell>
                    </TableRow>
                  );
                })}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}
