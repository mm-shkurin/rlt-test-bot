set -e

echo "Waiting for database..."
sleep 5

echo "Running migrations..."
alembic upgrade head

echo "Starting FastAPI application..."
uvicorn app.main:app --host 0.0.0.0 --port ${APP_PORT:-8000}

