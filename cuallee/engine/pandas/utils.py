from enum import Enum
from typing import Union, List, Any, Callable
from dataclasses import dataclass
import pandas as pd


class ComputeMethod(Enum):
    OBSERVE = "OBSERVE"
    SELECT = "SELECT"
    TRANSFORM = "TRANSFORM"


@dataclass
class ComputeInstruction:
    predicate: Union[Callable[[pd.DataFrame], pd.Series], List[Callable[[pd.DataFrame], pd.Series]], None]
    expression: Union[Callable[[pd.DataFrame], Any], None]
    compute_method: ComputeMethod

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"
