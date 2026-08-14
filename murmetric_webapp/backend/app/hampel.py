"""Filtre de Hampel recalculé à la volée — même algorithme exact que
`filtrer_hampel()`/`_filtrer_hampel_numpy()` dans ingestion_dewesoft_dxd.py
(médiane + MAD glissantes, effectif = max(MAD des valeurs, MAD des
différences successives), facteur 1.4826), mais appliqué ici à la demande
sur une fenêtre courte plutôt qu'une fois pour toutes à l'ingestion —
demande explicite de l'utilisateur (13/08/2026) : le réglage
HAMPEL_SEUIL_K/HAMPEL_FENETRE d'ingestion est fixe, pas ajustable depuis
l'interface. Ne modifie jamais les données stockées (`valeur_filtree`
reste tel quel en base) — un recalcul strictement pour l'affichage."""

import numpy as np

FACTEUR_MAD = 1.4826


def filtrer_hampel(
    valeurs: list[float], demi_fenetre: int, seuil_k: float
) -> tuple[list[float], list[bool]]:
    """Filtre de Hampel vectorisé (médiane + MAD glissantes) : retourne les valeurs
    corrigées et le masque des points jugés aberrants."""
    n = len(valeurs)
    v = np.asarray(valeurs, dtype=np.float64)
    filtrees = v.copy()
    aberrants = np.zeros(n, dtype=bool)
    largeur = 2 * demi_fenetre + 1

    if n >= largeur:
        vues = np.lib.stride_tricks.sliding_window_view(v, largeur)
        mediane = np.median(vues, axis=1)
        mad = np.median(np.abs(vues - mediane[:, None]), axis=1) * FACTEUR_MAD

        diffs = np.diff(vues, axis=1)
        mediane_diff = np.median(diffs, axis=1)
        mad_diff = np.median(np.abs(diffs - mediane_diff[:, None]), axis=1) * FACTEUR_MAD

        mad_effectif = np.maximum(mad, mad_diff)
        centre = v[demi_fenetre : demi_fenetre + vues.shape[0]]
        masque = (mad_effectif > 0) & (np.abs(centre - mediane) > seuil_k * mad_effectif)

        indices = np.nonzero(masque)[0] + demi_fenetre
        filtrees[indices] = mediane[np.nonzero(masque)[0]]
        aberrants[indices] = True

    # Bords (fenêtres tronquées) : traités un par un, comme la référence.
    bords = list(range(0, min(demi_fenetre, n))) + list(range(max(0, n - demi_fenetre), n))
    for i in sorted(set(bords)):
        lo, hi = max(0, i - demi_fenetre), min(n, i + demi_fenetre + 1)
        fenetre = valeurs[lo:hi]
        mediane_i = float(np.median(fenetre))
        mad_i = float(np.median(np.abs(np.array(fenetre) - mediane_i))) * FACTEUR_MAD

        diffs_fenetre = np.diff(fenetre)
        mad_diff_i = 0.0
        if diffs_fenetre.size:
            mediane_diff_i = float(np.median(diffs_fenetre))
            mad_diff_i = float(np.median(np.abs(diffs_fenetre - mediane_diff_i))) * FACTEUR_MAD

        mad_effectif_i = max(mad_i, mad_diff_i)
        if mad_effectif_i > 0 and abs(valeurs[i] - mediane_i) > seuil_k * mad_effectif_i:
            aberrants[i] = True
            filtrees[i] = mediane_i

    return filtrees.tolist(), aberrants.tolist()


def appliquer_bornes_physiques(
    valeurs: list[float],
    aberrants: list[bool],
    borne_min: float,
    borne_max: float,
) -> tuple[list[float], list[bool]]:
    """Deuxième couche, complémentaire du Hampel — demande explicite du
    13/08/2026, après avoir constaté sur données réelles qu'un pic positif
    extrême (+5898 mm) n'était pas corrigé par le Hampel seul : une rafale
    de plusieurs échantillons aberrants CONSÉCUTIFS plus longue que la
    fenêtre glissante rend la médiane locale elle-même corrompue (le
    Hampel ne "voit" que des valeurs cohérentes autour de lui). Une borne
    physique absolue, indépendante du contexte statistique local, rattrape
    ce cas — les points hors bornes sont remplacés par interpolation
    linéaire entre les voisins valides les plus proches (pas par la
    médiane locale, qui serait tout aussi corrompue dans ce scénario)."""
    v = np.asarray(valeurs, dtype=np.float64)
    n = len(v)
    hors_bornes = (v < borne_min) | (v > borne_max)
    tous_aberrants = np.asarray(aberrants, dtype=bool) | hors_bornes

    valide = ~hors_bornes
    if not valide.any() or not hors_bornes.any():
        return v.tolist(), tous_aberrants.tolist()

    indices = np.arange(n)
    idx_avant = np.maximum.accumulate(np.where(valide, indices, -1))
    idx_apres = np.minimum.accumulate(np.where(valide, indices, n)[::-1])[::-1]

    resultat = v.copy()
    for i in np.nonzero(hors_bornes)[0]:
        a, b = int(idx_avant[i]), int(idx_apres[i])
        if a == -1 and b == n:
            continue  # tout est hors bornes, rien de valide pour interpoler
        if a == -1:
            resultat[i] = v[b]
        elif b == n:
            resultat[i] = v[a]
        elif a == b:
            resultat[i] = v[a]
        else:
            t = (i - a) / (b - a)
            resultat[i] = v[a] + t * (v[b] - v[a])
    return resultat.tolist(), tous_aberrants.tolist()
