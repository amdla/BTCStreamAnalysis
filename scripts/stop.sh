#!/bin/bash

set -e

COMPOSE_FILES=(
  "docker/compose-storage.yaml"
  "docker/compose-jetstream.yaml"
  "docker/compose-streamserver.yaml"
)

echo "Stopping services..."
for file in "${COMPOSE_FILES[@]}"; do
  docker-compose -f "$file" down
done

echo "All services stopped"