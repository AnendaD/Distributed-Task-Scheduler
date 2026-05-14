#!/bin/sh
set -e

echo "Running migrations..."
/app/migrate -path=/app/migrations -datavase="$DATABASE_URL" up

echo "Starting app..."
exec /app/app