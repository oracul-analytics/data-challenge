# Feature Store + ML Sandbox

Простой feature-store поверх ClickHouse + LightGBM тренировка для детекта аномалий.

## Архитектура

- `domain` — сущности фичей, датасетов и артефактов модели.
- `application` — orchestrators для materialize/train/serve.
- `infrastructure` — ClickHouse, feature registry, LightGBM-тренер.
- `presentation` — CLI на Typer.

## Запуск

```bash
make infra-up           # ClickHouse (совместно с sandbox #1 можно юзать другой compose-порт)
make features           # материализуем фичи
make train              # тренируем модель
make serve              # пишем предсказания в витрину
```

Параметры лежат в `config/store.yaml`.

## Фичи

Фичи строятся по окну: count, mean, std, quantiles и rolling ratio. Registry в `infrastructure/registry.py`.

## Модель

LightGBM BinaryClassifier -> вероятность аномалии. Все артефакты валятся в `artifacts/`.

## Тесты

`make test` — smoke по registry и тренеру.
