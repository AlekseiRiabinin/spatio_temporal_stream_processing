#!/bin/bash
set -e

echo "=============================================="
echo "Starting Hive Metastore"
echo "=============================================="

echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."

until pg_isready \
    -h "$DB_HOST" \
    -p "$DB_PORT" \
    -U "$DB_USER"; do

    echo "PostgreSQL is not ready yet..."
    sleep 2
done

echo "PostgreSQL is available."

echo "Checking Hive Metastore schema..."

if schematool -dbType postgres -info; then

    echo "Hive Metastore schema already exists."

else

    echo "Hive Metastore schema does not exist."
    echo "Initializing schema..."

    schematool \
        -dbType postgres \
        -initSchema \
        --verbose

    echo "Hive Metastore schema initialized."

fi

echo "Verifying Hive Metastore schema..."

schematool \
    -dbType postgres \
    -info

echo "=============================================="
echo "Starting Hive Metastore on port 9083"
echo "=============================================="

exec hive --service metastore
