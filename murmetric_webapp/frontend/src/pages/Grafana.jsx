// Grafana embarqué en iframe (logique_projet.md section 32) : accès anonyme
// lecture seule + allow_embedding activés côté Grafana le 12/08/2026.
// HTTPS depuis le 19/08/2026 (reverse proxy Caddy, section 37 addendum) —
// requis notamment par Grafana Assistant (crypto.randomUUID indisponible
// hors contexte sécurisé). Domaine sslip.io gratuit (résout vers l'IP du
// VPS) ; à remplacer par un vrai nom de domaine plus tard si besoin, seule
// cette constante change.
const GRAFANA_BASE = "https://grafana.89-168-34-201.sslip.io";
const GRAFANA_URL = `${GRAFANA_BASE}/d/murmetric-hrt-socma?kiosk&theme=dark`;

export default function Grafana() {
  return (
    <div>
      <div className="carte">
        <p style={{ margin: 0, color: "#a0a6b5", fontSize: "0.85rem" }}>
          Vue intégrée en lecture seule (dashboard fixe). Pour composer tes propres graphiques (choisir librement
          mur/couche/canal/champ, créer de nouveaux panels), ouvre Grafana en plein écran et connecte-toi avec un compte
          admin — l'accès anonyme intégré ici reste volontairement limité à la lecture.
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
