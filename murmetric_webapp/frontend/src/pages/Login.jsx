import { useState } from "react";
import { api } from "../api.js";
import { auth } from "../auth.js";
import { Button } from "../components/ui/button.jsx";
import { Card, CardContent } from "../components/ui/card.jsx";
import { Input } from "../components/ui/input.jsx";
import { Label } from "../components/ui/label.jsx";

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
    <div className="flex min-h-screen items-center justify-center bg-slate-50">
      <Card className="w-80 shadow-lg">
        <CardContent className="pt-8 pb-8">
          <form onSubmit={soumettre} className="flex flex-col gap-6">
            <div className="flex justify-center mb-2">
              <LogoLight taille={48} />
            </div>
            <div className="flex flex-col gap-2">
              <Label htmlFor="login-username" className="text-sm font-semibold text-slate-900">
                Utilisateur
              </Label>
              <Input
                id="login-username"
                required
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                autoFocus
                className="text-base"
              />
            </div>
            <div className="flex flex-col gap-2">
              <Label htmlFor="login-password" className="text-sm font-semibold text-slate-900">
                Mot de passe
              </Label>
              <Input
                id="login-password"
                required
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="text-base"
              />
            </div>
            {erreur && <p className="text-sm text-destructive text-center">{erreur}</p>}
            <Button type="submit" disabled={enCours} className="w-full mt-2">
              {enCours ? "Connexion..." : "Se connecter"}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}

function LogoLight({ taille = 32 }) {
  return (
    <div
      style={{ display: "flex", alignItems: "center", gap: "0.6rem", justifyContent: "center", flexDirection: "column" }}
    >
      <svg width={taille} height={taille} viewBox="0 0 64 64" aria-hidden="true">
        <path
          d="M8 46 L22 18 L32 34 L46 10 L58 46"
          fill="none"
          stroke="#0f172a"
          strokeWidth="7"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <circle cx="46" cy="10" r="4.5" fill="#0f172a" />
      </svg>
      <div style={{ lineHeight: 1.15, textAlign: "center" }}>
        <div style={{ fontWeight: 700, fontSize: `${taille * 0.5}px`, color: "#0f172a" }}>MurMetric</div>
        <div style={{ fontSize: `${taille * 0.26}px`, color: "#64748b", letterSpacing: "0.03em" }}>by FRD-CODEM</div>
      </div>
    </div>
  );
}
