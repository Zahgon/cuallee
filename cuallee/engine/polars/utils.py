from enum import Enum
from typing import Union, List, Any
from dataclasses import dataclass
import polars as pl


class ComputeMethod(Enum):
    OBSERVE = "OBSERVE"
    SELECT = "SELECT"
    TRANSFORM = "TRANSFORM"


@dataclass
class ComputeInstruction:
    predicate: Union[pl.Expr, List[pl.Expr], None]
    expression: Union[pl.Expr, None]
    compute_method: ComputeMethod

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"
