-- Runs once, the first time the warehouse container starts with an empty volume.
-- `raw` holds data exactly as loaded from the source. dbt builds everything else.
CREATE SCHEMA IF NOT EXISTS raw;

-- TODO (Phase 1): design raw.resale_transactions here or create it from your load task.
-- Tip: keep raw columns as TEXT (load first, type-cast later in dbt staging),
-- and add a loaded_at TIMESTAMP so you can see when each row arrived.
