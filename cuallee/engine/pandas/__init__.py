import operator
from typing import Any, List, Union, Dict

import pandas as pd
from toolz import first

from ...core.check import Check, CheckStatus
from ...core.rule import Rule
from . import predicates as P
from . import utils as U


def dtypes(rules: List[Rule], dataframe: pd.DataFrame):
    """Validates affinity on data types prior to validation"""
    return True


def compute(rules: Dict[str, Rule]):
    """Computes the rules on the dataframe"""
    return {
        k: operator.methodcaller(v.method, v)(P) for k, v in rules.items()
    }


def summary(check: Check, dataframe: pd.DataFrame):
    
    # Compute the expressions
    computed_expressions = compute(check._rule)
    
    # Run computation
    result_dict = {}
    for hash_key, instruction in computed_expressions.items():
        if instruction.expression:
             # instruction.expression is a callable taking the dataframe
             result_dict[hash_key] = instruction.expression(dataframe)

    rows = len(dataframe)
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
    
    columns = [
        "id",
        "timestamp",
        "check",
        "level",
        "column",
        "rule",
        "value",
        "rows",
        "violations",
        "pass_rate",
        "pass_threshold",
        "status",
    ]

    return pd.DataFrame(data, columns=columns)


def ok(check: Check, dataframe: pd.DataFrame) -> bool:
    """True when all rules in the check pass validation"""
    df = summary(check, dataframe)
    return df["status"].eq(CheckStatus.PASS.value).all()
