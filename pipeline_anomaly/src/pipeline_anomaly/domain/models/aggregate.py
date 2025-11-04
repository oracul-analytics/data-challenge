from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping


@dataclass(frozen=True, slots=True)
class Aggregate:
    metric: str
    value: float
    window_start: datetime
    window_end: datetime
    extra: Mapping[str, float] | None = None


@dataclass(frozen=True, slots=True)
class AggregateCollection:
    """Набор агрегатов, рассчитанных по батчу."""

    aggregates: tuple[Aggregate, ...]

    def as_dict(self) -> list[dict[str, float | str]]:
        payload: list[dict[str, float | str]] = []
        for aggregate in self.aggregates:
            row: dict[str, float | str] = {
                "metric": aggregate.metric,
                "value": aggregate.value,
                "window_start": aggregate.window_start.isoformat(),
                "window_end": aggregate.window_end.isoformat(),
            }
            if aggregate.extra:
                row.update(
                    {f"extra_{key}": val for key, val in aggregate.extra.items()}
                )
            payload.append(row)
        return payload
