from .. import Check
import pandas as pd
from pathlib import Path
from typing import Dict


class BioChecks:
    def __init__(self, check: Check):
        self._check = check
        try:
            parent_path = Path(__file__).parent
            self._aminoacids = pd.read_csv(parent_path / "amino_acids.csv")
        except Exception:
            raise Exception("Unable to load aminoacid definitions")

    def is_dna(
        self,
        column: str,
        pct: float = 1.0,
        options: Dict[str, str] = {"name": "is_dna"},
    ):
        pass

    def is_protein(
        self,
        column: str,
        pct: float = 1.0,
        options: Dict[str, str] = {"name": "is_protein"},
    ):
        pass

    def is_cds(
        self,
        column: str,
        pct: float = 1.0,
        options: Dict[str, str] = {"name": "is_cds"},
    ):
        pass
