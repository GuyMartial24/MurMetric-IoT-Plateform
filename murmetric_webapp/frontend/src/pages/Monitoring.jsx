// Page "Monitoring" — santé des deux pipelines d'ingestion (retrait, hr_t) :
// fraîcheur des données InfluxDB + dernier battement de vie MQTT de chaque
// process, plus l'évolution de l'espace disque InfluxDB (section 32 de
// logique_projet.md pour le détail complet de l'architecture).
import { useEffect, useRef, useState } from "react";
import { api } from "../api.js";
import Pastille from "../components/Pastille.jsx";
import { Button } from "../components/ui/button.jsx";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card.jsx";
import { Skeleton } from "../components/ui/skeleton.jsx";

const COULEURS = {
  ok: "#4caf50",
  attention: "#ffa726",
  critique: "#ff5252",
  inactif: "#5a6270",
};

const LIBELLES_STATUT = {
  ok: "OK",
  attention: "Attention",
  critique: "Critique",
  inactif: "Inactif (aucune source activée)",
};

const LIBELLES_PIPELINE = {
  retrait: "Retrait (PC Amiens → DeweSoft)",
  hr_t: "Humidité / Température (Pi → BLE)",
};

const INTERVALLE_RAFRAICHISSEMENT_MS = 30_000;

function ilYA(dateIso) {
  if (!dateIso) return "jamais";
  const diffMin = Math.round((Date.now() - new Date(dateIso).getTime()) / 60000);
  if (diffMin < 60) return `il y a ${diffMin} min`;
  const diffH = Math.round(diffMin / 60);
  if (diffH < 48) return `il y a ${diffH} h`;
  return `il y a ${Math.round(diffH / 24)} j`;
}

export default function Monitoring() {
  const [etat, setEtat] = useState(null);
  const [erreur, setErreur] = useState(null);
  const [derniereActualisation, setDerniereActualisation] = useState(null);

  const charger = async () => {
    try {
      setEtat(await api.monitoringEtat());
      setErreur(null);
      setDerniereActualisation(new Date());
    } catch (e) {
      setErreur(e.message);
    }
  };

  useEffect(() => {
    charger();
    const id = setInterval(charger, INTERVALLE_RAFRAICHISSEMENT_MS);
    return () => clearInterval(id);
  }, []);

  return (
    <div>
      <div className="mb-2 flex items-center justify-between">
        <h2 className="m-0 text-lg font-semibold">Monitoring des pipelines d'ingestion</h2>
        <div className="flex items-center gap-3">
          {derniereActualisation && (
            <span className="text-xs text-muted-foreground">
              Actualisé {derniereActualisation.toLocaleTimeString("fr-FR")}
            </span>
          )}
          <Button variant="outline" size="sm" onClick={charger}>
            Actualiser
          </Button>
        </div>
      </div>
      <p className="text-sm text-muted-foreground">
        Fraîcheur des données réellement écrites en InfluxDB (seules les sources avec "ingestion" activé sont prises en
        compte) + dernier battement de vie reçu du process d'ingestion lui-même. Rafraîchi automatiquement toutes les
        30s.
      </p>
      {erreur && <p className="text-sm text-destructive">{erreur}</p>}
      <div className="mt-3 flex flex-col gap-5">
        {etat === null && !erreur && (
          <>
            <CartePipelineSquelette />
            <CartePipelineSquelette />
          </>
        )}
        {etat &&
          Object.entries(etat).map(([pipeline, infos]) => (
            <CartePipeline key={pipeline} pipeline={pipeline} infos={infos} />
          ))}
        <CarteEspaceDisque />
      </div>
    </div>
  );
}

function CartePipelineSquelette() {
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2.5">
          <Skeleton className="h-3 w-3 shrink-0 rounded-full" />
          <Skeleton className="h-4 w-48" />
        </div>
      </CardHeader>
      <CardContent className="flex flex-wrap gap-8">
        {Array.from({ length: 4 }, (_, i) => (
          <div key={i} className="flex flex-col gap-1.5">
            <Skeleton className="h-3 w-32" />
            <Skeleton className="h-4 w-24" />
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function formaterOctets(octets) {
  if (octets == null) return "?";
  return `${(octets / 1e9).toFixed(2)} Go`;
}

function CarteEspaceDisque() {
  const [donnees, setDonnees] = useState(null);
  const [erreur, setErreur] = useState(null);

  useEffect(() => {
    api
      .monitoringEspaceDisque(30)
      .then(setDonnees)
      .catch((e) => setErreur(e.message));
  }, []);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Espace disque InfluxDB</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <p className="text-sm text-muted-foreground">
          Mesuré toutes les 6h par une tâche cron sur le VPS (taille réelle sur disque, pas un nombre de points).
        </p>
        {erreur && <p className="text-sm text-destructive">{erreur}</p>}
        {donnees === null && !erreur && (
          <div className="flex flex-col gap-1.5">
            <Skeleton className="h-3 w-24" />
            <Skeleton className="h-4 w-40" />
            <Skeleton className="mt-2 h-[120px] w-full" />
          </div>
        )}
        {donnees && (
          <>
            <div className="text-sm">
              <div className="text-muted-foreground">Dernière mesure</div>
              <div>
                {formaterOctets(donnees.dernier_octets)}
                {donnees.mesure_le ? ` (${ilYA(donnees.mesure_le)})` : ""}
              </div>
            </div>
            <GraphiqueEspaceDisque points={donnees.points} />
          </>
        )}
      </CardContent>
    </Card>
  );
}

function GraphiqueEspaceDisque({ points }) {
  if (!points || points.length < 2) {
    return (
      <p className="text-sm text-muted-foreground">
        Pas encore assez de mesures pour tracer une évolution (une mesure toutes les 6h).
      </p>
    );
  }

  const largeur = 900,
    hauteur = 120,
    marge = 30;
  const temps = points.map((p) => new Date(p.time).getTime());
  const valeurs = points.map((p) => p.octets);
  const tMin = Math.min(...temps),
    tMax = Math.max(...temps);
  const vMin = Math.min(...valeurs),
    vMax = Math.max(...valeurs);

  const x = (t) => marge + ((t - tMin) / (tMax - tMin || 1)) * (largeur - 2 * marge);
  const y = (v) => hauteur - marge - ((v - vMin) / (vMax - vMin || 1)) * (hauteur - 2 * marge);
  const chemin = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(temps[i])},${y(valeurs[i])}`).join(" ");

  return (
    <div className="mt-3">
      <div className="mb-1 text-xs text-muted-foreground">Évolution sur les 30 derniers jours.</div>
      <svg viewBox={`0 0 ${largeur} ${hauteur}`} width="100%" height={hauteur}>
        <path d={chemin} fill="none" stroke="var(--ring)" strokeWidth="1.5" />
      </svg>
    </div>
  );
}

function CartePipeline({ pipeline, infos }) {
  const couleur = COULEURS[infos.statut] || "#5a6270";
  const hb = infos.heartbeat;

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2.5">
          <span className="inline-block h-3 w-3 shrink-0 rounded-full" style={{ background: couleur }} />
          <CardTitle>{LIBELLES_PIPELINE[pipeline] || pipeline}</CardTitle>
          <span className="text-sm font-semibold" style={{ color: couleur }}>
            {LIBELLES_STATUT[infos.statut] || infos.statut}
          </span>
        </div>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <div className="flex flex-wrap gap-8 text-sm">
          <div>
            <div className="text-muted-foreground">Dernière donnée reçue</div>
            <div>
              {infos.dernier_point
                ? `${new Date(infos.dernier_point).toLocaleString("fr-FR")} (${ilYA(infos.dernier_point)})`
                : "aucune"}
            </div>
          </div>
          <div>
            <div className="text-muted-foreground">Sources actives (ingestion activée)</div>
            <div>{infos.nb_sources_actives}</div>
          </div>
          <div>
            <div className="text-muted-foreground">Points reçus (24h, InfluxDB)</div>
            <div>{infos.points_24h != null ? infos.points_24h.toLocaleString("fr-FR") : "?"}</div>
          </div>
          {hb ? (
            <>
              <div>
                <div className="text-muted-foreground">Process ({hb.machine || "?"})</div>
                <div>en marche depuis {hb.demarre_le ? new Date(hb.demarre_le).toLocaleString("fr-FR") : "?"}</div>
              </div>
              <div>
                <div className="text-muted-foreground">Connexion MQTT</div>
                <div>
                  {hb.mqtt_connecte ? (
                    <Pastille etat="ok" texte="Connecté" />
                  ) : (
                    <Pastille etat="erreur" texte="Déconnecté" />
                  )}
                </div>
              </div>
              <div>
                <div className="text-muted-foreground">Buffer local en attente</div>
                <div>{hb.buffer_sqlite_en_attente ?? "?"} message(s)</div>
              </div>
              <div>
                <div className="text-muted-foreground">Registre capteurs (API)</div>
                <div>
                  {hb.registre_api_ok ? (
                    <Pastille etat="ok" texte="À jour" />
                  ) : (
                    <Pastille etat="attention" texte="Dernier appel en échec" />
                  )}
                </div>
              </div>
              <div>
                <div className="text-muted-foreground">Points publiés / bufferisés (cumul process)</div>
                <div>
                  {hb.nb_points_publies != null ? hb.nb_points_publies.toLocaleString("fr-FR") : "?"} /{" "}
                  {hb.nb_points_bufferises != null ? hb.nb_points_bufferises.toLocaleString("fr-FR") : "?"}
                </div>
              </div>
              <div>
                <div className="text-muted-foreground">Dernier battement</div>
                <div>{ilYA(hb.recu_le)}</div>
              </div>
            </>
          ) : (
            <div className="text-muted-foreground">
              Aucun battement de vie reçu (process jamais démarré depuis le déploiement du monitoring, ou trop ancien).
            </div>
          )}
        </div>

        {hb && <GraphiqueBuffer pipeline={pipeline} />}
      </CardContent>
    </Card>
  );
}

function GraphiqueBuffer({ pipeline }) {
  const [points, setPoints] = useState(null);
  const svgRef = useRef(null);

  useEffect(() => {
    api
      .monitoringHeartbeats(pipeline, 24)
      .then(setPoints)
      .catch(() => setPoints([]));
  }, [pipeline]);

  if (!points || points.length < 2) return null;

  const largeur = 900,
    hauteur = 120,
    marge = 30;
  const temps = points.map((p) => new Date(p.time).getTime());
  const valeurs = points.map((p) => p.buffer_sqlite_en_attente ?? 0);
  const tMin = Math.min(...temps),
    tMax = Math.max(...temps);
  const vMax = Math.max(1, ...valeurs);

  // Mise à l'échelle linéaire min/max (temps -> x, valeur -> y) dans le
  // viewBox SVG ; vMax borné à 1 pour éviter une division par zéro quand
  // le buffer est resté vide sur toute la fenêtre.
  const x = (t) => marge + ((t - tMin) / (tMax - tMin || 1)) * (largeur - 2 * marge);
  const y = (v) => hauteur - marge - (v / vMax) * (hauteur - 2 * marge);
  const chemin = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(temps[i])},${y(valeurs[i])}`).join(" ");

  return (
    <div className="mt-3">
      <div className="mb-1 text-xs text-muted-foreground">
        Messages en attente dans le buffer local (24 dernières heures) — une valeur qui grimpe et ne redescend pas
        signale une perte de connexion au cloud prolongée.
      </div>
      <svg ref={svgRef} viewBox={`0 0 ${largeur} ${hauteur}`} width="100%" height={hauteur}>
        <path d={chemin} fill="none" stroke="var(--ring)" strokeWidth="1.5" />
      </svg>
    </div>
  );
}
