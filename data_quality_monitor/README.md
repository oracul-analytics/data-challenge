# Data Quality Monitoring Sandbox

FastAPI-сервис для автоматической проверки качества данных: completeness, uniqueness, value ranges, schema drift.

## Архитектура

- `domain` — правила валидации и результаты проверок.
- `application` — orchestrator для запуска проверок по конфигу.
- `infrastructure` — ClickHouse client, rule registry, Kafka mock (через файл), FastAPI endpoints.
- `presentation` — REST API и CLI-триггеры.

## Запуск

```bash
make infra-up
make api
make run-checks
```

## Конфиг

`config/rules.yaml` описывает таблицы и правила:

```yaml
rules:
  - table: default.events
    expectations:
      - type: completeness
        column: value
        threshold: 0.99
```

Сервис хранит результаты в ClickHouse таблице `dq_reports` и возвращает их через `/reports`.

## Мониторинг

- логирование через loguru
- прометеевские метрики (экспорт `/metrics`)

## Тесты

`make test` — unit по движку правил.
