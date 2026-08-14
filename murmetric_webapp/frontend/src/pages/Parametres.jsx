// Page "Paramètres" — deux blocs indépendants : identifiants des fournisseurs
// IA (Gemini principal, Groq en repli texte, cf. routers/assistant.py) et
// gestion du compte de l'utilisateur connecté. Les clés API ne sont jamais
// réaffichées en clair par le backend (cf. parametres.masquer).
import { useEffect, useState } from "react";
import { api } from "../api.js";
import { auth } from "../auth.js";

function joursRestants(dateISO) {
  if (!dateISO) return null;
  const diff = new Date(dateISO).getTime() - Date.now();
  return Math.ceil(diff / (1000 * 60 * 60 * 24));
}

function ParametresIA() {
  const [modeleGroq, setModeleGroq] = useState("");
  const [cleGroqActuelle, setCleGroqActuelle] = useState("");
  const [nouvelleCleGroq, setNouvelleCleGroq] = useState("");
  const [expiration, setExpiration] = useState("");
  const [modeleGemini, setModeleGemini] = useState("");
  const [cleGeminiActuelle, setCleGeminiActuelle] = useState("");
  const [nouvelleCleGemini, setNouvelleCleGemini] = useState("");
  const [erreur, setErreur] = useState(null);
  const [message, setMessage] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const charger = async () => {
    try {
      const p = await api.lireParametres();
      setCleGroqActuelle(p.groq_api_key_masque);
      setModeleGroq(p.groq_model);
      setExpiration(p.groq_api_key_expiration || "");
      setCleGeminiActuelle(p.gemini_api_key_masque);
      setModeleGemini(p.gemini_model);
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
      await api.modifierParametres({
        groq_api_key: nouvelleCleGroq || null,
        groq_model: modeleGroq || null,
        groq_api_key_expiration: expiration || null,
        gemini_api_key: nouvelleCleGemini || null,
        gemini_model: modeleGemini || null,
      });
      setNouvelleCleGroq("");
      setNouvelleCleGemini("");
      setMessage("Paramètres enregistrés.");
      await charger();
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  const restants = joursRestants(expiration);

  return (
    <div className="carte">
      <h2>Identifiants API — Assistant IA</h2>
      <p style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>
        Gemini est le fournisseur principal (texte + analyse d'image de graphique) — Groq prend le relais
        automatiquement si Gemini est indisponible pour une question textuelle (pas possible pour l'analyse d'image,
        propre à Gemini).
      </p>
      <form onSubmit={enregistrer}>
        <h3>Gemini (Google AI Studio)</h3>
        <div className="champ">
          <label>Clé API actuelle</label>
          <input value={cleGeminiActuelle} disabled />
        </div>
        <div className="champ">
          <label>Nouvelle clé API (laisser vide pour ne pas changer)</label>
          <input
            value={nouvelleCleGemini}
            onChange={(e) => setNouvelleCleGemini(e.target.value)}
            placeholder="AQ...."
          />
        </div>
        <div className="champ">
          <label>Modèle</label>
          <input
            value={modeleGemini}
            onChange={(e) => setModeleGemini(e.target.value)}
            placeholder="gemini-flash-latest"
          />
        </div>

        <h3 style={{ marginTop: "1rem" }}>Groq (repli texte)</h3>
        <div className="champ">
          <label>Clé API actuelle</label>
          <input value={cleGroqActuelle} disabled />
        </div>
        <div className="champ">
          <label>Nouvelle clé API (laisser vide pour ne pas changer)</label>
          <input value={nouvelleCleGroq} onChange={(e) => setNouvelleCleGroq(e.target.value)} placeholder="gsk_..." />
        </div>
        <div className="champ">
          <label>Modèle</label>
          <input
            value={modeleGroq}
            onChange={(e) => setModeleGroq(e.target.value)}
            placeholder="llama-3.3-70b-versatile"
          />
        </div>
        <div className="champ">
          <label>Date d'expiration de la clé Groq (informative — saisie manuelle)</label>
          <input type="date" value={expiration} onChange={(e) => setExpiration(e.target.value)} />
        </div>
        {restants != null && (
          <p style={{ color: restants < 30 ? "#ff8080" : "#a0a6b5" }}>
            {restants >= 0
              ? `Expire dans ${restants} jour(s).`
              : `Expirée depuis ${-restants} jour(s) — pense à la renouveler.`}
          </p>
        )}
        {erreur && <p className="erreur">{erreur}</p>}
        {message && <p>{message}</p>}
        <button type="submit" disabled={enCours}>
          {enCours ? "Enregistrement..." : "Enregistrer"}
        </button>
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
      // Le backend renvoie un nouveau jeton JWT si le username a changé (l'ancien
      // jeton référence l'ancien username) — toujours réappliqué, même sans
      // changement, pour rester en un seul chemin de code.
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
          <input
            required
            type="password"
            value={motDePasseActuel}
            onChange={(e) => setMotDePasseActuel(e.target.value)}
          />
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
        <button type="submit" disabled={enCours}>
          {enCours ? "Enregistrement..." : "Enregistrer"}
        </button>
      </form>
    </div>
  );
}

export default function Parametres() {
  return (
    <div>
      <MonCompte />
      <ParametresIA />
    </div>
  );
}
