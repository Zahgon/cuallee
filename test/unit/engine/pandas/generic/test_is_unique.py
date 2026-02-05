import pytest
import pandas as pd
from cuallee import Check, CheckLevel

def test_is_unique():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "id_dup": [1, 1, 3, 4, 5]})
    check = Check(CheckLevel.WARNING, "pandas")
    check.is_unique("id")
    result = check.validate(df)
    assert result.status.str.match("PASS").all()

def test_is_unique_fail():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "id_dup": [1, 1, 3, 4, 5]})
    check = Check(CheckLevel.WARNING, "pandas")
    check.is_unique("id_dup")
    result = check.validate(df)
    assert result.status.str.match("FAIL").all()
