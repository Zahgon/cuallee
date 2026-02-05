import pytest
import polars as pl
from cuallee import Check, CheckLevel


@pytest.fixture
def check():
    return Check(CheckLevel.WARNING, "test_check")


@pytest.fixture
def df():
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "id_null": [1, 2, None, 4, 5]
    })


def test_is_complete(check, df):
    check.is_complete("id")
    result = check.validate(df)
    assert result.select(pl.col("status").eq("PASS")).to_series().all()


def test_is_complete_fail(check, df):
    check.is_complete("id_null")
    result = check.validate(df)
    assert not result.select(pl.col("status").eq("PASS")).to_series().all()
