#!/usr/bin/env bash
# Local dev runner for hosts without make (Docker-only rule: no host installs).
# The Makefile stays the canonical target list for CI/contributors; keep both in sync.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f deploy/docker-compose.yml"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh
BROKER=localhost:9092

cmd="${1:-help}"
case "$cmd" in
  up)      $COMPOSE up -d ;;
  down)    $COMPOSE down ;;
  ps)      $COMPOSE ps ;;
  logs)    $COMPOSE logs -f --tail=100 ;;
  dev)     $COMPOSE --profile dev up -d --build --wait ;;
  topics)
    for t in order.events inventory.events payment.events notification.events; do
      $COMPOSE exec -T kafka $KAFKA_TOPICS --bootstrap-server "$BROKER" --create --if-not-exists --topic "$t" --partitions 3
    done
    $COMPOSE exec -T kafka $KAFKA_TOPICS --bootstrap-server "$BROKER" --list
    ;;
  subjects) curl -s http://localhost:8081/subjects | sort ;;
  schemas) ./scripts/register-schemas.sh ;;
  clean)   $COMPOSE down -v ;;
  *) echo "usage: scripts/dev.sh {up|down|ps|logs|dev|topics|subjects|clean}" ;;
esac
