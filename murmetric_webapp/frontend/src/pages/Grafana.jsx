// Grafana embarqué en iframe (logique_projet.md section 32) : accès anonyme
// lecture seule + allow_embedding activés côté Grafana le 12/08/2026.
// URL en dur pour l'instant (IP publique du VPS, port 3000) — à revoir si
// Grafana passe un jour par un proxy same-origin plutôt qu'un port public
// séparé.
const GRAFANA_URL = "http://89.168.34.201:3000/d/murmetric-hrt-socma?kiosk&theme=dark";

export default function Grafana() {
  return (
    <div className="carte" style={{ padding: 0, overflow: "hidden" }}>
      <iframe
        title="Dashboards Grafana MurMetric"
        src={GRAFANA_URL}
        style={{ width: "100%", height: "80vh", border: "none" }}
      />
    </div>
  );
}
