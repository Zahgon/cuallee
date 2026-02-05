import pytest
import pandas as pd
from cuallee import Check, CheckLevel

def test_is_between():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "val": [10, 20, 30, 40, 50]})
    check = Check(CheckLevel.WARNING, "pandas")
    check.is_between("val", (10, 50))
    result = check.validate(df)
    assert result.status.str.match("PASS").all()

def test_is_between_fail():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "val": [10, 20, 30, 40, 60]})
    check = Check(CheckLevel.WARNING, "pandas")
    check.is_between("val", (10, 50))
    result = check.validate(df)
    assert result.status.str.match("FAIL").all()
