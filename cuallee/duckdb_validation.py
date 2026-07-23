import operator
from functools import reduce
from numbers import Number

import duckdb as dk
import numpy as np
import pandas as pd  # type: ignore
from toolz import first  # type: ignore
from string import Template
import re
import textwrap

from cuallee import Check, Rule, CheckStatus


class Compute:
    def __init__(self, table_name: str = None):
        self.table_name = table_name

    def is_complete(self, rule: Rule) -> str:
        """Verify the absence of null values in a column"""
        return f"SUM(CAST({rule.column} IS NOT NULL AS INTEGER))"

    def is_empty(self, rule: Rule) -> str:
        pass

    def are_complete(self, rule: Rule) -> str:
        pass

    def is_unique(self, rule: Rule) -> str:
        """Confirms the absence of duplicate values in a column"""
        return f"COUNT(DISTINCT({rule.column}))"

    def are_unique(self, rule: Rule) -> str:
        pass

    def is_greater_than(self, rule: Rule) -> str:
        pass

    def is_less_than(self, rule: Rule) -> str:
        pass

    def is_greater_or_equal_than(self, rule: Rule) -> str:
        pass

    def is_less_or_equal_than(self, rule: Rule) -> str:
        pass

    def is_equal_than(self, rule: Rule) -> str:
        pass

    def has_pattern(self, rule: Rule) -> str:
        pass

    def has_min(self, rule: Rule) -> str:
        pass

    def has_max(self, rule: Rule) -> str:
        pass

    def has_std(self, rule: Rule) -> str:
        pass

    def has_mean(self, rule: Rule) -> str:
        pass

    def has_sum(self, rule: Rule) -> str:
        pass

    def has_cardinality(self, rule: Rule) -> str:
        pass

    def has_infogain(self, rule: Rule) -> str:
        pass

    def is_between(self, rule: Rule) -> str:
        pass

    def is_contained_in(self, rule: Rule) -> str:
        pass

    def not_contained_in(self, rule: Rule) -> str:
        pass

    def has_percentile(self, rule: Rule) -> str:
        pass

    def has_max_by(self, rule: Rule) -> str:
        pass

    def has_min_by(self, rule: Rule) -> str:
        pass

    def has_correlation(self, rule: Rule) -> str:
        pass

    def satisfies(self, rule: Rule) -> str:
        pass

    def has_entropy(self, rule: Rule) -> str:
        pass

    def is_on_weekday(self, rule: Rule) -> str:
        pass

    def is_on_weekend(self, rule: Rule) -> str:
        pass

    def is_on_monday(self, rule: Rule) -> str:
        pass

    def is_on_tuesday(self, rule: Rule) -> str:
        pass

    def is_on_wednesday(self, rule: Rule) -> str:
        pass

    def is_on_thursday(self, rule: Rule) -> str:
        pass

    def is_on_friday(self, rule: Rule) -> str:
        pass

    def is_on_saturday(self, rule: Rule) -> str:
        pass

    def is_on_sunday(self, rule: Rule) -> str:
        pass

    def is_on_schedule(self, rule: Rule) -> str:
        pass

    def is_daily(self, rule: Rule) -> str:
        pass

    def is_inside_interquartile_range(self, rule: Rule) -> str:
        pass

    def has_workflow(self, rule: Rule) -> str:
        pass


def validate_data_types(check: Check, dataframe: dk.DuckDBPyConnection):
    return True


def compute(check: Check):
    return True


def summary(check: Check, connection: dk.DuckDBPyConnection) -> list:
    if isinstance(connection, dk.DuckDBPyRelation):
        raise NotImplementedError(
            textwrap.dedent(
                """
        Invalid DuckDb object, please pass a DuckDbPyConnection instead. And register your relation like:
        conn = duckdb.connect()
        check = Check(table_name="demo_table")
        duckdb_relation_object = conn.sql("FROM range(10)")
        conn.register(view_name=check.table_name, python_object=duckdb_relation_object)
        check.is_complete("range").validate(conn)
        """
            )
        )

    unified_columns = ",\n\t".join(
        [
            operator.methodcaller(rule.method, rule)(Compute(check.table_name))
            + f" AS '{rule.key}'"
            for rule in check.rules
        ]
    )
    unified_query = f"""
    SELECT
    \t{unified_columns}
    FROM
    \t'{check.table_name}'
    """

    def _calculate_violations(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 0
            else:
                return nrows
        elif isinstance(result, Number):
            return nrows - result
        elif isinstance(result, (list, np.ndarray)):
            if len(result) == 2:
                return result[1]

    def _calculate_pass_rate(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 1.0
            else:
                return 0.0
        elif isinstance(result, Number):
            return result / nrows
        elif isinstance(result, (list, np.ndarray)):
            if result[1] > 0:
                if result[1] > nrows:
                    return nrows / result[1]
                else:
                    return result[1] / nrows
            else:
                return 1.0

    def _evaluate_status(pass_rate, pass_threshold):
        if pass_rate >= pass_threshold:
            return "PASS"
        else:
            return "FAIL"

    _merge_dicts = lambda a, b: {**a, **b}
    unified_results = reduce(
        _merge_dicts, connection.execute(unified_query).df().to_dict(orient="records")
    )

    rows = first(
        connection.execute(f"select count(*) from '{check.table_name}'").fetchone()
    )

    computation_basis = [
        {
            "id": index,
            "timestamp": check.date.strftime("%Y-%m-%d %H:%M:%S"),
            "check": check.name,
            "level": check.level.name,
            "column": rule.column,
            "rule": rule.name,
            "value": rule.value,
            "rows": rows,
            "violations": _calculate_violations(unified_results[hash_key], rows),
            "pass_rate": _calculate_pass_rate(unified_results[hash_key], rows),
            "pass_threshold": rule.coverage,
            "status": _evaluate_status(
                _calculate_pass_rate(unified_results[hash_key], rows),
                rule.coverage,
            ),
        }
        for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
    ]
    return pd.DataFrame(computation_basis).reset_index(drop=True)


def ok(check: Check, connection: dk.DuckDBPyConnection) -> bool:
    """True when all rules in the check pass validation"""
    return summary(check, connection).status.str.match(CheckStatus.PASS.value).all()
