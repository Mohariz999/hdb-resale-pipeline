-- Used by CI only: the same raw table the DAG's load task creates, filled from a small sample.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE raw.resale_transactions (
    month TEXT,
    town TEXT,
    flat_type TEXT,
    block TEXT,
    street_name TEXT,
    storey_range TEXT,
    floor_area_sqm TEXT,
    flat_model TEXT,
    lease_commence_date TEXT,
    remaining_lease TEXT,
    resale_price TEXT,
    loaded_at TIMESTAMP NOT NULL DEFAULT now()
);
\copy raw.resale_transactions (month, town, flat_type, block, street_name, storey_range, floor_area_sqm, flat_model, lease_commence_date, remaining_lease, resale_price) FROM 'ci/sample_resale_transactions.csv' WITH (FORMAT csv, HEADER true)
