#!/bin/bash

USER="postgres"
DBS=("fragment0" "fragment1" "fragment2")

echo "Dropping databases..."

for db in "${DBS[@]}"; do
  psql -U $USER -c "DROP DATABASE IF EXISTS $db;"
done

echo "Creating databases..."

for db in "${DBS[@]}"; do
  psql -U $USER -c "CREATE DATABASE $db;"
done

echo "Loading schema..."

for db in "${DBS[@]}"; do
  psql -U $USER -d $db -f script.sql
done

echo "Done."
