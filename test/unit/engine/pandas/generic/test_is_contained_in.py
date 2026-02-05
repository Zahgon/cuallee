import pytest
import pandas as pd
from cuallee import Check, CheckLevel

def test_is_contained_in():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "val": ["a", "b", "c", "d", "e"]})
    check = Check(CheckLevel.WARNING, "pandas")
    check.is_contained_in("val", ["a", "b", "c", "d", "e", "f"])
    result = check.validate(df)
    assert result.status.str.match("PASS").all()

def test_is_contained_in_fail():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "val": ["a", "b", "c", "d", "z"]})
    check = Check(CheckLevel.WARNING, "pandas")
    check.is_contained_in("val", ["a", "b", "c", "d", "e"])
    result = check.validate(df)
    assert result.status.str.match("FAIL").all()
