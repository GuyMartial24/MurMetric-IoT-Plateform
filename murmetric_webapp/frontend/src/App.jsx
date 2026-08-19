import { useState } from "react";
import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import Logo from "./components/Logo.jsx";
import { FournisseurEtatPages } from "./EtatPagesContext.jsx";
import Assistant from "./pages/Assistant.jsx";
import Capteurs from "./pages/Capteurs.jsx";
import Export from "./pages/Export.jsx";
import Grafana from "./pages/Grafana.jsx";
import Login from "./pages/Login.jsx";
import Monitoring from "./pages/Monitoring.jsx";
import Parametres from "./pages/Parametres.jsx";
import TeneurEau from "./pages/TeneurEau.jsx";
import VueEnsemble from "./pages/VueEnsemble.jsx";
import { auth } from "./auth.js";

const ONGLETS = [
  { chemin: "/", label: "Vue d'ensemble" },
  { chemin: "/grafana", label: "Grafana" },
  { chemin: "/teneur-eau", label: "Teneur en eau" },
  { chemin: "/export", label: "Export" },
  { chemin: "/capteurs", label: "Capteurs" },
  { chemin: "/monitoring", label: "Monitoring" },
  { chemin: "/assistant", label: "Assistant IA" },
  { chemin: "/parametres", label: "Paramètres" },
];

// Garde de route simple : redirige vers /login plutôt que de rendre la page
// protégée si l'utilisateur n'est pas connecté (état géré au niveau App, pas
// de contexte React dédié — un seul niveau d'imbrication, pas nécessaire).
function EspaceProtege({ connecte, children }) {
  return connecte ? children : <Navigate to="/login" replace />;
}

export default function App() {
  const [connecte, setConnecte] = useState(auth.estConnecte());

  const deconnecter = () => {
    auth.deconnecter();
    setConnecte(false);
  };

  if (!connecte) {
    return (
      <Routes>
        <Route path="*" element={<Login onConnecte={() => setConnecte(true)} />} />
      </Routes>
    );
  }

  return (
    <FournisseurEtatPages>
      <div className="app">
        <header className="app-header">
          <Logo taille={30} />
          <nav>
            {ONGLETS.map((onglet) => (
              <NavLink key={onglet.chemin} to={onglet.chemin} end={onglet.chemin === "/"}>
                {onglet.label}
              </NavLink>
            ))}
          </nav>
          <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: "0.75rem" }}>
            <span style={{ color: "#a0a6b5", fontSize: "0.85rem" }}>{auth.getNomAffiche()}</span>
            <button onClick={deconnecter}>Déconnexion</button>
          </div>
        </header>
        <main className="app-main">
          <Routes>
            <Route
              path="/"
              element={
                <EspaceProtege connecte={connecte}>
                  <VueEnsemble />
                </EspaceProtege>
              }
            />
            <Route
              path="/grafana"
              element={
                <EspaceProtege connecte={connecte}>
                  <Grafana />
                </EspaceProtege>
              }
            />
            <Route
              path="/teneur-eau"
              element={
                <EspaceProtege connecte={connecte}>
                  <TeneurEau />
                </EspaceProtege>
              }
            />
            <Route
              path="/export"
              element={
                <EspaceProtege connecte={connecte}>
                  <Export />
                </EspaceProtege>
              }
            />
            <Route
              path="/capteurs"
              element={
                <EspaceProtege connecte={connecte}>
                  <Capteurs />
                </EspaceProtege>
              }
            />
            <Route
              path="/monitoring"
              element={
                <EspaceProtege connecte={connecte}>
                  <Monitoring />
                </EspaceProtege>
              }
            />
            <Route
              path="/assistant"
              element={
                <EspaceProtege connecte={connecte}>
                  <Assistant />
                </EspaceProtege>
              }
            />
            <Route
              path="/parametres"
              element={
                <EspaceProtege connecte={connecte}>
                  <Parametres />
                </EspaceProtege>
              }
            />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>
      </div>
    </FournisseurEtatPages>
  );
}
