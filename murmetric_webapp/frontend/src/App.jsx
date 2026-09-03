import { useState } from "react";
import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import { Menu } from "lucide-react";
import Logo from "./components/Logo.jsx";
import { Button } from "./components/ui/button.jsx";
import { Avatar, AvatarFallback } from "./components/ui/avatar.jsx";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "./components/ui/dropdown-menu.jsx";
import { Sheet, SheetContent, SheetTitle, SheetTrigger } from "./components/ui/sheet.jsx";
import { cn } from "./lib/utils.js";
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

const lienNavClasses = ({ isActive }) =>
  cn(
    "border-b-2 border-transparent py-1.5 text-sm text-muted-foreground no-underline transition-colors hover:text-foreground",
    isActive && "border-ring text-foreground",
  );

function ListeNav({ className, onNaviguer }) {
  return (
    <nav className={className}>
      {ONGLETS.map((onglet) => (
        <NavLink
          key={onglet.chemin}
          to={onglet.chemin}
          end={onglet.chemin === "/"}
          onClick={onNaviguer}
          className={lienNavClasses}
        >
          {onglet.label}
        </NavLink>
      ))}
    </nav>
  );
}

// Initiales pour l'avatar du menu compte — 1re lettre des 2 premiers mots du
// nom affiché, ou ses 2 premières lettres si un seul mot. getNomAffiche()
// peut renvoyer null (lu directement depuis localStorage).
function initiales(nom) {
  if (!nom) return "?";
  const mots = nom.trim().split(/\s+/);
  if (mots.length === 1) return mots[0].slice(0, 2).toUpperCase();
  return (mots[0][0] + mots[1][0]).toUpperCase();
}

function ComptePanel({ className, onDeconnecter }) {
  const nom = auth.getNomAffiche();
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" className={cn("gap-2 px-2", className)}>
          <Avatar className="h-7 w-7">
            <AvatarFallback className="text-xs">{initiales(nom)}</AvatarFallback>
          </Avatar>
          <span className="hidden text-sm text-muted-foreground sm:inline">{nom}</span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onClick={onDeconnecter}>Déconnexion</DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

export default function App() {
  const [connecte, setConnecte] = useState(auth.estConnecte());
  const [menuOuvert, setMenuOuvert] = useState(false);

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
        <header className="entete-accent flex items-center gap-8 bg-primary px-6 py-3">
          <Logo taille={30} />
          <ListeNav className="hidden items-center gap-5 md:flex" />
          <ComptePanel className="ml-auto hidden md:flex" onDeconnecter={deconnecter} />
          <Sheet open={menuOuvert} onOpenChange={setMenuOuvert}>
            <SheetTrigger asChild>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="ml-auto md:hidden"
                aria-label="Basculer le menu de navigation"
              >
                <Menu className="h-5 w-5" />
              </Button>
            </SheetTrigger>
            <SheetContent side="left" className="flex w-72 flex-col gap-6">
              <SheetTitle className="sr-only">Menu de navigation</SheetTitle>
              <ListeNav className="flex flex-col gap-2" onNaviguer={() => setMenuOuvert(false)} />
              <ComptePanel className="flex" onDeconnecter={deconnecter} />
            </SheetContent>
          </Sheet>
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
