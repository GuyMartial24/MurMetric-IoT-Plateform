const CLE_JETON = "murmetric_token";
const CLE_NOM = "murmetric_nom_affiche";

export const auth = {
  getToken: () => localStorage.getItem(CLE_JETON),
  getNomAffiche: () => localStorage.getItem(CLE_NOM),
  estConnecte: () => Boolean(localStorage.getItem(CLE_JETON)),
  connecter: (token, nomAffiche) => {
    localStorage.setItem(CLE_JETON, token);
    localStorage.setItem(CLE_NOM, nomAffiche);
  },
  deconnecter: () => {
    localStorage.removeItem(CLE_JETON);
    localStorage.removeItem(CLE_NOM);
  },
};
