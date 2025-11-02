POETRY := $(shell which poetry)
PYTHON=$(POETRY) run python
UVICORN=$(POETRY) run uvicorn

.PHONY: install infra-up infra-down api run-checks test

install:
	$(POETRY) install --no-root

infra-up:
	docker compose up -d

infra-down:
	docker compose down -v

api:
	$(UVICORN) data_quality_monitor.presentation.api:app --host 0.0.0.0 --port 8080 --reload

run-checks:
	$(PYTHON) -m data_quality_monitor.presentation.cli run --config config/rules.yaml

test:
	$(POETRY) run pytest
