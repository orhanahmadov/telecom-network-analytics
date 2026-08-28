.PHONY: up down clean test

up:
	cp -n .env.example .env || true
	docker compose -f infrastructure/docker/docker-compose.yml up -d

down:
	docker compose -f infrastructure/docker/docker-compose.yml down

clean:
	docker compose -f infrastructure/docker/docker-compose.yml down -v
	find . -type d -name "__pycache__" -exec rm -rf {} +

test:
	pytest tests/
