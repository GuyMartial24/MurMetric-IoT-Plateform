import { useEffect, useRef, useState } from "react";
import { api } from "../api.js";
import Pastille from "../components/Pastille.jsx";

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
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "0.5rem" }}>
        <h2 style={{ margin: 0 }}>Monitoring des pipelines d'ingestion</h2>
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
          {derniereActualisation && (
            <span style={{ color: "#a0a6b5", fontSize: "0.8rem" }}>
              Actualisé {derniereActualisation.toLocaleTimeString("fr-FR")}
            </span>
          )}
          <button onClick={charger}>Actualiser</button>
        </div>
      </div>
      <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
        Fraîcheur des données réellement écrites en InfluxDB (seules les sources avec "ingestion" activé sont
        prises en compte) + dernier battement de vie reçu du process d'ingestion lui-même. Rafraîchi automatiquement
        toutes les 30s.
      </p>
      {erreur && <p className="erreur">{erreur}</p>}
      {etat && Object.entries(etat).map(([pipeline, infos]) => (
        <CartePipeline key={pipeline} pipeline={pipeline} infos={infos} />
      ))}
      <CarteEspaceDisque />
    </div>
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
    api.monitoringEspaceDisque(30).then(setDonnees).catch((e) => setErreur(e.message));
  }, []);

  return (
    <div className="carte">
      <h3 style={{ marginTop: 0 }}>Espace disque InfluxDB</h3>
      <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
        Mesuré toutes les 6h par une tâche cron sur le VPS (taille réelle sur disque, pas un nombre de points).
      </p>
      {erreur && <p className="erreur">{erreur}</p>}
      {donnees && (
        <>
          <div style={{ fontSize: "0.88rem" }}>
            <div style={{ color: "#a0a6b5" }}>Dernière mesure</div>
            <div>
              {formaterOctets(donnees.dernier_octets)}
              {donnees.mesure_le ? ` (${ilYA(donnees.mesure_le)})` : ""}
            </div>
          </div>
          <GraphiqueEspaceDisque points={donnees.points} />
        </>
      )}
    </div>
  );
}

function GraphiqueEspaceDisque({ points }) {
  if (!points || points.length < 2) {
    return (
      <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
        Pas encore assez de mesures pour tracer une évolution (une mesure toutes les 6h).
      </p>
    );
  }

  const largeur = 900, hauteur = 120, marge = 30;
  const temps = points.map((p) => new Date(p.time).getTime());
  const valeurs = points.map((p) => p.octets);
  const tMin = Math.min(...temps), tMax = Math.max(...temps);
  const vMin = Math.min(...valeurs), vMax = Math.max(...valeurs);

  const x = (t) => marge + ((t - tMin) / (tMax - tMin || 1)) * (largeur - 2 * marge);
  const y = (v) => hauteur - marge - ((v - vMin) / (vMax - vMin || 1)) * (hauteur - 2 * marge);
  const chemin = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(temps[i])},${y(valeurs[i])}`).join(" ");

  return (
    <div style={{ marginTop: "0.75rem" }}>
      <div style={{ color: "#a0a6b5", fontSize: "0.78rem", marginBottom: "0.25rem" }}>
        Évolution sur les 30 derniers jours.
      </div>
      <svg viewBox={`0 0 ${largeur} ${hauteur}`} width="100%" height={hauteur}>
        <path d={chemin} fill="none" stroke="#7fd4ff" strokeWidth="1.5" />
      </svg>
    </div>
  );
}

function CartePipeline({ pipeline, infos }) {
  const couleur = COULEURS[infos.statut] || "#5a6270";
  const hb = infos.heartbeat;

  return (
    <div className="carte">
      <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "0.5rem" }}>
        <span style={{ width: "12px", height: "12px", borderRadius: "50%", background: couleur, display: "inline-block" }} />
        <h3 style={{ margin: 0 }}>{LIBELLES_PIPELINE[pipeline] || pipeline}</h3>
        <span style={{ color: couleur, fontWeight: 600, fontSize: "0.9rem" }}>{LIBELLES_STATUT[infos.statut] || infos.statut}</span>
      </div>

      <div style={{ display: "flex", flexWrap: "wrap", gap: "2rem", fontSize: "0.88rem" }}>
        <div>
          <div style={{ color: "#a0a6b5" }}>Dernière donnée reçue</div>
          <div>{infos.dernier_point ? `${new Date(infos.dernier_point).toLocaleString("fr-FR")} (${ilYA(infos.dernier_point)})` : "aucune"}</div>
        </div>
        <div>
          <div style={{ color: "#a0a6b5" }}>Sources actives (ingestion activée)</div>
          <div>{infos.nb_sources_actives}</div>
        </div>
        <div>
          <div style={{ color: "#a0a6b5" }}>Points reçus (24h, InfluxDB)</div>
          <div>{infos.points_24h != null ? infos.points_24h.toLocaleString("fr-FR") : "?"}</div>
        </div>
        {hb ? (
          <>
            <div>
              <div style={{ color: "#a0a6b5" }}>Process ({hb.machine || "?"})</div>
              <div>en marche depuis {hb.demarre_le ? new Date(hb.demarre_le).toLocaleString("fr-FR") : "?"}</div>
            </div>
            <div>
              <div style={{ color: "#a0a6b5" }}>Connexion MQTT</div>
              <div>{hb.mqtt_connecte ? <Pastille etat="ok" texte="Connecté" /> : <Pastille etat="erreur" texte="Déconnecté" />}</div>
            </div>
            <div>
              <div style={{ color: "#a0a6b5" }}>Buffer local en attente</div>
              <div>{hb.buffer_sqlite_en_attente ?? "?"} message(s)</div>
            </div>
            <div>
              <div style={{ color: "#a0a6b5" }}>Registre capteurs (API)</div>
              <div>{hb.registre_api_ok ? <Pastille etat="ok" texte="À jour" /> : <Pastille etat="attention" texte="Dernier appel en échec" />}</div>
            </div>
            <div>
              <div style={{ color: "#a0a6b5" }}>Points publiés / bufferisés (cumul process)</div>
              <div>
                {hb.nb_points_publies != null ? hb.nb_points_publies.toLocaleString("fr-FR") : "?"} /{" "}
                {hb.nb_points_bufferises != null ? hb.nb_points_bufferises.toLocaleString("fr-FR") : "?"}
              </div>
            </div>
            <div>
              <div style={{ color: "#a0a6b5" }}>Dernier battement</div>
              <div>{ilYA(hb.recu_le)}</div>
            </div>
          </>
        ) : (
          <div style={{ color: "#a0a6b5" }}>Aucun battement de vie reçu (process jamais démarré depuis le déploiement du monitoring, ou trop ancien).</div>
        )}
      </div>

      {hb && <GraphiqueBuffer pipeline={pipeline} />}
    </div>
  );
}

function GraphiqueBuffer({ pipeline }) {
  const [points, setPoints] = useState(null);
  const svgRef = useRef(null);

  useEffect(() => {
    api.monitoringHeartbeats(pipeline, 24).then(setPoints).catch(() => setPoints([]));
  }, [pipeline]);

  if (!points || points.length < 2) return null;

  const largeur = 900, hauteur = 120, marge = 30;
  const temps = points.map((p) => new Date(p.time).getTime());
  const valeurs = points.map((p) => p.buffer_sqlite_en_attente ?? 0);
  const tMin = Math.min(...temps), tMax = Math.max(...temps);
  const vMax = Math.max(1, ...valeurs);

  const x = (t) => marge + ((t - tMin) / (tMax - tMin || 1)) * (largeur - 2 * marge);
  const y = (v) => hauteur - marge - (v / vMax) * (hauteur - 2 * marge);
  const chemin = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(temps[i])},${y(valeurs[i])}`).join(" ");

  return (
    <div style={{ marginTop: "0.75rem" }}>
      <div style={{ color: "#a0a6b5", fontSize: "0.78rem", marginBottom: "0.25rem" }}>
        Messages en attente dans le buffer local (24 dernières heures) — une valeur qui grimpe et ne redescend pas
        signale une perte de connexion au cloud prolongée.
      </div>
      <svg ref={svgRef} viewBox={`0 0 ${largeur} ${hauteur}`} width="100%" height={hauteur}>
        <path d={chemin} fill="none" stroke="#7fd4ff" strokeWidth="1.5" />
      </svg>
    </div>
  );
}
