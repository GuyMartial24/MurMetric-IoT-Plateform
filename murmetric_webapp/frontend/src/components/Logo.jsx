// Marque "MurMetric" — mark (silhouette de paroi/signal capteur en zigzag,
// point = mesure) + wordmark "MurMetric" + tagline "by FRD-CODEM". Même
// SVG que public/favicon.svg (dupliqué inline plutôt qu'importé : couleur
// figée, pas de dépendance à un fichier statique pour ce composant).
export default function Logo({ taille = 32, centre = false }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", justifyContent: centre ? "center" : "flex-start" }}>
      <svg width={taille} height={taille} viewBox="0 0 64 64" aria-hidden="true">
        <path d="M8 46 L22 18 L32 34 L46 10 L58 46" fill="none" stroke="#7fd4ff" strokeWidth="7" strokeLinecap="round" strokeLinejoin="round" />
        <circle cx="46" cy="10" r="4.5" fill="#7fd4ff" />
      </svg>
      <div style={{ lineHeight: 1.15 }}>
        <div style={{ fontWeight: 700, fontSize: `${taille * 0.5}px`, color: "#fff" }}>MurMetric</div>
        <div style={{ fontSize: `${taille * 0.26}px`, color: "#a0a6b5", letterSpacing: "0.03em" }}>by FRD-CODEM</div>
      </div>
    </div>
  );
}
