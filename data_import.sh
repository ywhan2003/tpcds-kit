#!/bin/bash

DB_USER="postgres"
DB_NAME="tpcds"
DATA_DIR="/home/ubuntu/tpcds-data"
HOST="localhost"

TABLES=(
    "call_center" "catalog_page" "catalog_returns" "catalog_sales"
    "customer" "customer_address" "customer_demographics" "date_dim"
    "household_demographics" "income_band" "inventory" "item"
    "promotion" "reason" "ship_mode" "store" "store_returns"
    "store_sales" "time_dim" "warehouse" "web_page" "web_returns"
    "web_sales" "web_site"
)

for TABLE in "${TABLES[@]}"; do
    echo "Importing $TABLE..."
    psql -U $DB_USER -d $DB_NAME -h $HOST -c "\COPY $TABLE FROM '$DATA_DIR/${TABLE}.dat' WITH DELIMITER '|' NULL '';"
done

echo "Data import completed!"