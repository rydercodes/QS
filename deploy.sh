#!/bin/bash

# Create required directories
mkdir -p ./dags ./logs ./plugins ./config

# Set correct permissions
if [ -z "$AIRFLOW_UID" ]; then
    export AIRFLOW_UID=$(id -u)
    export AIRFLOW_GID=$(id -g)
fi

# Stop any running containers
docker-compose down -v

# Remove old images
docker-compose rm -f
docker system prune -f

# Build new images
docker-compose build --no-cache

# Start services
docker-compose up -d

# Wait for services to be healthy
echo "Waiting for services to be ready..."
TIMEOUT=120
START_TIME=$(date +%s)

while ! docker-compose ps | grep airflow-webserver | grep "healthy"; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED_TIME -gt $TIMEOUT ]; then
        echo "Timeout waiting for services to be ready"
        docker-compose logs airflow-webserver
        exit 1
    fi
    
    echo "Still waiting... ($ELAPSED_TIME seconds elapsed)"
    sleep 5
done

echo "Services are ready!"
echo "Access Airflow at http://localhost:8080"
echo "Username: admin"
echo "Password: admin"

# Show logs if requested
if [ "$1" == "--logs" ]; then
    docker-compose logs -f
fi