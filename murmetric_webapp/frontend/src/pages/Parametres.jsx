// Page "Paramètres" — deux blocs indépendants : identifiants des fournisseurs
// IA (Gemini principal, Groq en repli texte, cf. routers/assistant.py) et
// gestion du compte de l'utilisateur connecté. Les clés API ne sont jamais
// réaffichées en clair par le backend (cf. parametres.masquer).
import { useEffect, useState } from "react";
import { api } from "../api.js";
import { auth } from "../auth.js";
import { Button } from "../components/ui/button.jsx";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card.jsx";
import { Input } from "../components/ui/input.jsx";
import { Label } from "../components/ui/label.jsx";
import { cn } from "../lib/utils.js";

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
    <Card>
      <CardHeader>
        <CardTitle>Identifiants API — Assistant IA</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground">
          Gemini est le fournisseur principal (texte + analyse d'image de graphique) — Groq prend le relais
          automatiquement si Gemini est indisponible pour une question textuelle (pas possible pour l'analyse d'image,
          propre à Gemini).
        </p>
        <form onSubmit={enregistrer} className="mt-3 flex flex-col gap-3">
          <h3 className="m-0 text-base font-medium">Gemini (Google AI Studio)</h3>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Clé API actuelle</Label>
            <Input value={cleGeminiActuelle} disabled />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">
              Nouvelle clé API (laisser vide pour ne pas changer)
            </Label>
            <Input
              value={nouvelleCleGemini}
              onChange={(e) => setNouvelleCleGemini(e.target.value)}
              placeholder="AQ...."
            />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Modèle</Label>
            <Input
              value={modeleGemini}
              onChange={(e) => setModeleGemini(e.target.value)}
              placeholder="gemini-flash-latest"
            />
          </div>

          <h3 className="m-0 mt-2 text-base font-medium">Groq (repli texte)</h3>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Clé API actuelle</Label>
            <Input value={cleGroqActuelle} disabled />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">
              Nouvelle clé API (laisser vide pour ne pas changer)
            </Label>
            <Input value={nouvelleCleGroq} onChange={(e) => setNouvelleCleGroq(e.target.value)} placeholder="gsk_..." />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Modèle</Label>
            <Input
              value={modeleGroq}
              onChange={(e) => setModeleGroq(e.target.value)}
              placeholder="llama-3.3-70b-versatile"
            />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">
              Date d'expiration de la clé Groq (informative — saisie manuelle)
            </Label>
            <Input type="date" value={expiration} onChange={(e) => setExpiration(e.target.value)} />
          </div>
          {restants != null && (
            <p className={cn("text-sm", restants < 30 ? "text-destructive" : "text-muted-foreground")}>
              {restants >= 0
                ? `Expire dans ${restants} jour(s).`
                : `Expirée depuis ${-restants} jour(s) — pense à la renouveler.`}
            </p>
          )}
          {erreur && <p className="text-sm text-destructive">{erreur}</p>}
          {message && <p className="text-sm">{message}</p>}
          <Button type="submit" disabled={enCours} className="self-start">
            {enCours ? "Enregistrement..." : "Enregistrer"}
          </Button>
        </form>
      </CardContent>
    </Card>
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
    <Card>
      <CardHeader>
        <CardTitle>Modifier mon compte</CardTitle>
      </CardHeader>
      <CardContent>
        <form onSubmit={enregistrer} className="flex flex-col gap-3">
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">
              Mot de passe actuel (obligatoire pour confirmer)
            </Label>
            <Input
              required
              type="password"
              value={motDePasseActuel}
              onChange={(e) => setMotDePasseActuel(e.target.value)}
            />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Nouveau nom d'utilisateur (optionnel)</Label>
            <Input value={nouveauUsername} onChange={(e) => setNouveauUsername(e.target.value)} />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Nouveau mot de passe (optionnel)</Label>
            <Input type="password" value={nouveauPassword} onChange={(e) => setNouveauPassword(e.target.value)} />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Nom affiché (optionnel)</Label>
            <Input value={nouveauNomAffiche} onChange={(e) => setNouveauNomAffiche(e.target.value)} />
          </div>
          {erreur && <p className="text-sm text-destructive">{erreur}</p>}
          {message && <p className="text-sm">{message}</p>}
          <Button type="submit" disabled={enCours} className="self-start">
            {enCours ? "Enregistrement..." : "Enregistrer"}
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

function CreerCompte() {
  const [nouveauUsername, setNouveauUsername] = useState("");
  const [nouveauPassword, setNouveauPassword] = useState("");
  const [nomAffiche, setNomAffiche] = useState("");
  const [erreur, setErreur] = useState(null);
  const [message, setMessage] = useState(null);
  const [enCours, setEnCours] = useState(false);

  const creer = async (e) => {
    e.preventDefault();
    setEnCours(true);
    setErreur(null);
    setMessage(null);
    try {
      await api.register(nouveauUsername, nouveauPassword, nomAffiche);
      setMessage(`Compte "${nouveauUsername}" créé.`);
      setNouveauUsername("");
      setNouveauPassword("");
      setNomAffiche("");
    } catch (e) {
      setErreur(e.message);
    } finally {
      setEnCours(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Créer un compte</CardTitle>
      </CardHeader>
      <CardContent>
        <form onSubmit={creer} className="flex flex-col gap-3">
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Nom d'utilisateur</Label>
            <Input required value={nouveauUsername} onChange={(e) => setNouveauUsername(e.target.value)} />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Mot de passe</Label>
            <Input
              required
              type="password"
              value={nouveauPassword}
              onChange={(e) => setNouveauPassword(e.target.value)}
            />
          </div>
          <div className="flex flex-col gap-1">
            <Label className="text-xs font-normal text-muted-foreground">Nom affiché (optionnel)</Label>
            <Input value={nomAffiche} onChange={(e) => setNomAffiche(e.target.value)} />
          </div>
          {erreur && <p className="text-sm text-destructive">{erreur}</p>}
          {message && <p className="text-sm">{message}</p>}
          <Button type="submit" disabled={enCours} className="self-start">
            {enCours ? "Création..." : "Créer le compte"}
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

export default function Parametres() {
  return (
    <div className="flex flex-col gap-5">
      <MonCompte />
      <CreerCompte />
      <ParametresIA />
    </div>
  );
}
