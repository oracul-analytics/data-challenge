POETRY := $(shell which poetry)
PYTHON=$(POETRY) run python

.PHONY: install infra-up infra-down pipeline test format lint

install:
	$(POETRY) install --no-root

infra-up:
	docker compose up -d

infra-down:
	docker compose down -v

pipeline:
	$(PYTHON) -m pipeline_anomaly.presentation.cli --config config/pipeline.yaml

pipeline-%:
	$(PYTHON) -m pipeline_anomaly.presentation.cli run --config config/$*.yaml

test:
	$(POETRY) run pytest
