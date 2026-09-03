import { Button } from "../components/ui/button.jsx";
import { Card, CardContent } from "../components/ui/card.jsx";

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
    <div className="flex flex-col gap-6">
      <Card>
        <CardContent className="flex flex-col gap-3">
          <p className="text-sm text-muted-foreground">
            Vue intégrée en lecture seule (dashboard fixe). Pour composer tes propres graphiques (choisir librement
            mur/couche/canal/champ, créer de nouveaux panels), ouvre Grafana en plein écran et connecte-toi avec un
            compte admin — l'accès anonyme intégré ici reste volontairement limité à la lecture.
          </p>
          <a href={GRAFANA_BASE} target="_blank" rel="noreferrer" className="self-start">
            <Button>Ouvrir Grafana en plein écran ↗</Button>
          </a>
        </CardContent>
      </Card>
      <Card className="p-0">
        <iframe title="Dashboards Grafana MurMetric" src={GRAFANA_URL} className="h-[75vh] w-full border-none" />
      </Card>
    </div>
  );
}
