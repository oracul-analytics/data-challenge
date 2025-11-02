POETRY := $(shell which poetry)
PYTHON=$(POETRY) run python

.PHONY: install infra-up infra-down features train serve test

install:
	$(POETRY) install --no-root

infra-up:
	docker compose up -d

infra-down:
	docker compose down -v

features:
	$(PYTHON) -m feature_store_ml.presentation.cli materialize --config config/store.yaml

train:
	$(PYTHON) -m feature_store_ml.presentation.cli train --config config/store.yaml

serve:
	$(PYTHON) -m feature_store_ml.presentation.cli serve --config config/store.yaml

test:
	$(POETRY) run pytest
