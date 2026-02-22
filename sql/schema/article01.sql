CREATE SCHEMA IF NOT EXISTS cre;

CREATE TABLE IF NOT EXISTS cre.window_results (
    id SERIAL PRIMARY KEY,
    window_start BIGINT NOT NULL,
    window_end   BIGINT NOT NULL,
    geohash TEXT NOT NULL,
    value BIGINT NOT NULL,
    processing_time BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_window_results_geohash ON window_results(geohash);
CREATE INDEX idx_window_results_window_start ON window_results(window_start);
