#!/bin/bash

set -e

COMPOSE_FILES=(
  "docker/compose-storage.yaml"
  "docker/compose-jetstream.yaml"
  "docker/compose-streamserver.yaml"
)

PATTERNS=(
  "Mongo Express server listening"
  "Server is ready"
  "JetStream context initialized"
)

NAMES=(
  "Storage"
  "JetStream"
  "StreamServer"
)

echo "Building and starting services..."

for i in "${!COMPOSE_FILES[@]}"; do
  echo -e "\nStarting ${NAMES[$i]}..."
  docker-compose -f "${COMPOSE_FILES[$i]}" up --build -d
  docker-compose -f "${COMPOSE_FILES[$i]}" logs -f | grep -q "${PATTERNS[$i]}" &
  wait $!
  echo "${NAMES[$i]} ready"
done

echo -e "\nAll services are up and running!"