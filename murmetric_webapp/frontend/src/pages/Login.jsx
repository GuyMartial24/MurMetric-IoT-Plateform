import { useState } from "react";
import { api } from "../api.js";
import { auth } from "../auth.js";
import Logo from "../components/Logo.jsx";

export default function Login({ onConnecte }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [erreur, setErreur] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const soumettre = async (e) => {
    e.preventDefault();
    setEnCours(true);
    setErreur(null);
    try {
      const resultat = await api.login(username, password);
      auth.connecter(resultat.access_token, resultat.nom_affiche);
      onConnecte();
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <div style={{ display: "flex", justifyContent: "center", alignItems: "center", minHeight: "100vh" }}>
      <form onSubmit={soumettre} className="carte" style={{ width: "320px" }}>
        <div style={{ marginBottom: "1.25rem" }}>
          <Logo taille={40} centre />
        </div>
        <div className="champ">
          <label>Utilisateur</label>
          <input required value={username} onChange={(e) => setUsername(e.target.value)} autoFocus />
        </div>
        <div className="champ">
          <label>Mot de passe</label>
          <input required type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
        </div>
        {erreur && <p className="erreur">{erreur}</p>}
        <button type="submit" disabled={enCours} style={{ width: "100%" }}>
          {enCours ? "Connexion..." : "Se connecter"}
        </button>
      </form>
    </div>
  );
}
