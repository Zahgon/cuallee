import pytest
import pandas as pd
from cuallee import Check, CheckLevel

def test_is_complete():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "id_null": [1, 2, None, 4, 5]})
    check = Check(CheckLevel.WARNING, "pandas")
    check.is_complete("id")
    result = check.validate(df)
    assert result.status.str.match("PASS").all()

def test_is_complete_fail():
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "id_null": [1, 2, None, 4, 5]})
    check = Check(CheckLevel.WARNING, "pandas")
    check.is_complete("id_null")
    result = check.validate(df)
    assert result.status.str.match("FAIL").all()
