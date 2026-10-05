#!/usr/bin/env bash
set -euo pipefail

# Let the Airflow container write to the mounted folders as your user.
if [ ! -f .env ]; then
  echo "AIRFLOW_UID=$(id -u)" > .env
fi

# dbt in the Codespace terminal too, so you can run `dbt build` without going through Airflow.
pip install --user --no-cache-dir "dbt-postgres==1.11.0"
if ! grep -q DBT_PROFILES_DIR ~/.bashrc; then
  echo "export DBT_PROFILES_DIR=$PWD/dbt" >> ~/.bashrc
fi

echo ""
echo "Setup done. Next: docker compose up -d --build"
