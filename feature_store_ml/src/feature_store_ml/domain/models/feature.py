from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd


@dataclass(frozen=True)
class Feature:
    
    name: str
    compute_fn: Callable[[pd.DataFrame], pd.Series]
    
    def compute(self, dataframe: pd.DataFrame) -> pd.Series:
        return self.compute_fn(dataframe)
    
    def __repr__(self) -> str:
        return f"Feature(name='{self.name}')"
    