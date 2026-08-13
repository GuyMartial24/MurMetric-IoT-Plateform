// Grafana embarqué en iframe (logique_projet.md section 32) : accès anonyme
// lecture seule + allow_embedding activés côté Grafana le 12/08/2026.
// URL en dur pour l'instant (IP publique du VPS, port 3000) — à revoir si
// Grafana passe un jour par un proxy same-origin plutôt qu'un port public
// séparé.
const GRAFANA_BASE = "http://89.168.34.201:3000";
const GRAFANA_URL = `${GRAFANA_BASE}/d/murmetric-hrt-socma?kiosk&theme=dark`;

export default function Grafana() {
  return (
    <div>
      <div className="carte">
        <p style={{ margin: 0, color: "#a0a6b5", fontSize: "0.85rem" }}>
          Vue intégrée en lecture seule (dashboard fixe). Pour composer tes propres graphiques
          (choisir librement mur/couche/canal/champ, créer de nouveaux panels), ouvre Grafana en
          plein écran et connecte-toi avec un compte admin — l'accès anonyme intégré ici reste
          volontairement limité à la lecture (Grafana est public sur Internet).
        </p>
        <a href={GRAFANA_BASE} target="_blank" rel="noreferrer">
          <button style={{ marginTop: "0.75rem" }}>Ouvrir Grafana en plein écran ↗</button>
        </a>
      </div>
      <div className="carte" style={{ padding: 0, overflow: "hidden" }}>
        <iframe
          title="Dashboards Grafana MurMetric"
          src={GRAFANA_URL}
          style={{ width: "100%", height: "75vh", border: "none" }}
        />
      </div>
    </div>
  );
}
