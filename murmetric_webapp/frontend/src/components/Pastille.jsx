const COULEURS = {
  ok: "#4caf50",
  attention: "#ffa726",
  erreur: "#ff5252",
  neutre: "#5a6270",
};

export default function Pastille({ etat, texte }) {
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: "0.4rem" }}>
      <span
        style={{
          width: "8px",
          height: "8px",
          borderRadius: "50%",
          background: COULEURS[etat] || COULEURS.neutre,
          display: "inline-block",
          flexShrink: 0,
        }}
      />
      {texte}
    </span>
  );
}
