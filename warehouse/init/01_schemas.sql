-- Runs once, the first time the warehouse container starts with an empty volume.
-- `raw` holds data exactly as loaded from the source. dbt builds everything else.
CREATE SCHEMA IF NOT EXISTS raw;

-- raw.resale_transactions itself is created by the DAG's load task (all TEXT columns, plus loaded_at).
