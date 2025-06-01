#!/usr/bin/env bash

echo "Remove all containers."
docker compose down -v --remove-orphans
sleep 1

rm -f docker-compose.yml
echo "Done."
