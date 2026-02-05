import operator
from typing import Any, List, Union, Dict

import polars as pl
from toolz import first

from ...core.check import Check, CheckStatus
from ...core.rule import Rule
from . import predicates as P
from . import utils as U


def dtypes(rules: List[Rule], dataframe: pl.DataFrame):
    """Validates affinity on data types prior to validation"""
    return True


def compute(rules: Dict[str, Rule]):
    """Computes the rules on the dataframe"""
    return {
        k: operator.methodcaller(v.method, v)(P) for k, v in rules.items()
    }


def summary(check: Check, dataframe: pl.DataFrame):
    
    # Compute the expressions
    computed_expressions = compute(check._rule)
    
    # Collect all expressions
    # Both OBSERVE and SELECT in Polars can be computed via select() since they return scalars (aggregations)
    expressions = [
        instruction.expression.alias(hash_key) 
        for hash_key, instruction in computed_expressions.items()
    ]
    
    # Run computation
    if expressions:
        result_dict = dataframe.select(expressions).to_dicts()[0]
    else:
        result_dict = {}

    rows = dataframe.height
    check.rows = rows

    # Evaluate rules
    for index, (hash_key, rule) in enumerate(check._rule.items(), 1):
        rule.ordinal = index
        rule.evaluate(result_dict[hash_key], rows)

    # Create result dataframe
    data = [
        (
            rule.ordinal,
            check.date.strftime("%Y-%m-%d %H:%M:%S"),
            check.name,
            check.level.name,
            str(rule.column),
            str(rule.name),
            str(rule.format_value()),
            int(check.rows),
            int(rule.violations),
            float(rule.pass_rate),
            float(rule.coverage),
            rule.status,
        )
        for rule in check.rules
    ]
    
    schema = [
        ("id", pl.Int64),
        ("timestamp", pl.Utf8),
        ("check", pl.Utf8),
        ("level", pl.Utf8),
        ("column", pl.Utf8),
        ("rule", pl.Utf8),
        ("value", pl.Utf8),
        ("rows", pl.Int64),
        ("violations", pl.Int64),
        ("pass_rate", pl.Float64),
        ("pass_threshold", pl.Float64),
        ("status", pl.Utf8),
    ]

    return pl.DataFrame(data, schema=schema, orient="row")


def ok(check: Check, dataframe: pl.DataFrame) -> bool:
    """True when all rules in the check pass validation"""
    df = summary(check, dataframe)
    return df.select(pl.col("status").eq(CheckStatus.PASS.value)).to_series().all()
