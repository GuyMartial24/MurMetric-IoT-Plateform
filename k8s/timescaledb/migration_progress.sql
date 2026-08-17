-- Table de suivi de la migration InfluxDB -> TimescaleDB (Phase 1, section 32).
-- Un lot = une mesure + une valeur de tag (canal/MAC/etc.) + un jour.
-- verifie=true seulement après comparaison des comptages source/cible réussie
-- — permet de relancer le script de migration sans jamais dupliquer un lot
-- déjà migré et vérifié, et de repérer précisément les lots en écart.
CREATE TABLE IF NOT EXISTS migration_progress (
    mesure TEXT NOT NULL,
    tag_valeur TEXT NOT NULL,
    jour DATE NOT NULL,
    nb_points_source BIGINT,
    nb_points_migres BIGINT,
    verifie BOOLEAN NOT NULL DEFAULT false,
    migre_le TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (mesure, tag_valeur, jour)
);
