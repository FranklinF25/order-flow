# Local runner on hosts without make: scripts/dev.sh (keep both in sync).
COMPOSE := docker compose -f deploy/docker-compose.yml
KAFKA_TOPICS := /opt/kafka/bin/kafka-topics.sh
BROKER := localhost:9092
SR := http://localhost:8081

.PHONY: up down dev ps logs topics subjects clean

up:
	$(COMPOSE) up -d

dev:
	$(COMPOSE) --profile dev up -d --build --wait

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

# Event streams per topology v1 (DLQ topics arrive with M2 retry logic)
topics:
	$(COMPOSE) exec kafka $(KAFKA_TOPICS) --bootstrap-server $(BROKER) --create --if-not-exists --topic order.events        --partitions 3
	$(COMPOSE) exec kafka $(KAFKA_TOPICS) --bootstrap-server $(BROKER) --create --if-not-exists --topic inventory.events    --partitions 3
	$(COMPOSE) exec kafka $(KAFKA_TOPICS) --bootstrap-server $(BROKER) --create --if-not-exists --topic payment.events      --partitions 3
	$(COMPOSE) exec kafka $(KAFKA_TOPICS) --bootstrap-server $(BROKER) --create --if-not-exists --topic notification.events --partitions 3
	$(COMPOSE) exec kafka $(KAFKA_TOPICS) --bootstrap-server $(BROKER) --list

subjects:
	@curl -s $(SR)/subjects | sort

clean:
	$(COMPOSE) down -v

# Dev loop (Docker-only rule): Go services run as compose containers
# (profile "dev", air hot-reload, source mounted) — never on the host.
# Optional host ports for direct debugging:
#   order :8082 | inventory :8083 | payment :8084
# Gateway :8088 (8080 taken by llama-server) | Schema Registry :8081 | RabbitMQ mgmt :15672
