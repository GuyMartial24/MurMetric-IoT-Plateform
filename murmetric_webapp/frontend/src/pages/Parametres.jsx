import { useEffect, useState } from "react";
import { api } from "../api.js";
import { auth } from "../auth.js";

function ParametresGroq() {
  const [modele, setModele] = useState("");
  const [cleActuelle, setCleActuelle] = useState("");
  const [nouvelleCle, setNouvelleCle] = useState("");
  const [erreur, setErreur] = useState(null);
  const [message, setMessage] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const charger = async () => {
    try {
      const p = await api.lireParametres();
      setCleActuelle(p.groq_api_key_masque);
      setModele(p.groq_model);
    } catch (e) {
      setErreur(e.message);
    }
  };

  useEffect(() => {
    charger();
  }, []);

  const enregistrer = async (e) => {
    e.preventDefault();
    setEnCours(true);
    setErreur(null);
    setMessage(null);
    try {
      await api.modifierParametres({ groq_api_key: nouvelleCle || null, groq_model: modele || null });
      setNouvelleCle("");
      setMessage("Paramètres enregistrés.");
      await charger();
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <div className="carte">
      <h2>Assistant IA (Groq)</h2>
      <form onSubmit={enregistrer}>
        <div className="champ">
          <label>Clé API actuelle</label>
          <input value={cleActuelle} disabled />
        </div>
        <div className="champ">
          <label>Nouvelle clé API (laisser vide pour ne pas changer)</label>
          <input value={nouvelleCle} onChange={(e) => setNouvelleCle(e.target.value)} placeholder="gsk_..." />
        </div>
        <div className="champ">
          <label>Modèle</label>
          <input value={modele} onChange={(e) => setModele(e.target.value)} placeholder="llama-3.3-70b-versatile" />
        </div>
        {erreur && <p className="erreur">{erreur}</p>}
        {message && <p>{message}</p>}
        <button type="submit" disabled={enCours}>{enCours ? "Enregistrement..." : "Enregistrer"}</button>
      </form>
    </div>
  );
}

function MonCompte() {
  const [motDePasseActuel, setMotDePasseActuel] = useState("");
  const [nouveauUsername, setNouveauUsername] = useState("");
  const [nouveauPassword, setNouveauPassword] = useState("");
  const [nouveauNomAffiche, setNouveauNomAffiche] = useState("");
  const [erreur, setErreur] = useState(null);
  const [message, setMessage] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const enregistrer = async (e) => {
    e.preventDefault();
    setEnCours(true);
    setErreur(null);
    setMessage(null);
    try {
      const resultat = await api.modifierCompte({
        mot_de_passe_actuel: motDePasseActuel,
        nouveau_username: nouveauUsername || null,
        nouveau_password: nouveauPassword || null,
        nouveau_nom_affiche: nouveauNomAffiche || null,
      });
      auth.connecter(resultat.access_token, nouveauNomAffiche || auth.getNomAffiche());
      setMotDePasseActuel("");
      setNouveauUsername("");
      setNouveauPassword("");
      setNouveauNomAffiche("");
      setMessage("Compte mis à jour.");
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <div className="carte">
      <h2>Mon compte</h2>
      <form onSubmit={enregistrer}>
        <div className="champ">
          <label>Mot de passe actuel (obligatoire pour confirmer)</label>
          <input required type="password" value={motDePasseActuel} onChange={(e) => setMotDePasseActuel(e.target.value)} />
        </div>
        <div className="champ">
          <label>Nouveau nom d'utilisateur (optionnel)</label>
          <input value={nouveauUsername} onChange={(e) => setNouveauUsername(e.target.value)} />
        </div>
        <div className="champ">
          <label>Nouveau mot de passe (optionnel)</label>
          <input type="password" value={nouveauPassword} onChange={(e) => setNouveauPassword(e.target.value)} />
        </div>
        <div className="champ">
          <label>Nom affiché (optionnel)</label>
          <input value={nouveauNomAffiche} onChange={(e) => setNouveauNomAffiche(e.target.value)} />
        </div>
        {erreur && <p className="erreur">{erreur}</p>}
        {message && <p>{message}</p>}
        <button type="submit" disabled={enCours}>{enCours ? "Enregistrement..." : "Enregistrer"}</button>
      </form>
    </div>
  );
}

export default function Parametres() {
  return (
    <div>
      <MonCompte />
      <ParametresGroq />
    </div>
  );
}
