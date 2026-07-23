import daft
import operator
import statistics
import numpy as np
import pandas as pd

from math import log2
from toolz import first
from typing import Union
from numbers import Number
from typing import Dict, List

from itertools import groupby
from operator import itemgetter

from cuallee import Check, Rule


class Compute:

    def is_complete(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        col_name = daft.col(rule.column)
        perdicate = col_name.not_null().cast(daft.DataType.int64())
        return dataframe.select(perdicate).sum(col_name).to_pandas().iloc[0, 0]

    def is_empty(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def are_complete(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_unique(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        perdicate = daft.col(rule.column)
        return (
            dataframe.select(perdicate)
            .distinct()
            .count(perdicate)
            .to_pandas()
            .iloc[0, 0]
        )

    def are_unique(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_greater_than(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        pass

    def is_greater_or_equal_than(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        pass

    def is_less_than(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_less_or_equal_than(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        pass

    def is_equal_than(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_pattern(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_min(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_max(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_std(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_mean(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_sum(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_cardinality(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        pass

    def has_infogain(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_between(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_contained_in(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        pass

    def not_contained_in(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        pass

    def has_percentile(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_max_by(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_min_by(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_correlation(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        pass

    def satisfies(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def has_entropy(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_on_weekday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_on_weekend(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_on_monday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_on_tuesday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_on_wednesday(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, int]:
        pass

    def is_on_thursday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_on_friday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_on_saturday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_on_sunday(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_on_schedule(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass

    def is_daily(self, rule: Rule, dataframe: daft.DataFrame) -> complex:
        pass

    def is_inside_interquartile_range(
        self, rule: Rule, dataframe: daft.DataFrame
    ) -> Union[bool, complex]:
        pass

    def has_workflow(self, rule: Rule, dataframe: daft.DataFrame) -> Union[bool, int]:
        pass


def compute(rules: Dict[str, Rule]):
    """Daft computes directly on the predicates"""
    return True


def validate_data_types(rules: List[Rule], dataframe: daft.DataFrame):
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""
    return True


def summary(check: Check, dataframe: daft.DataFrame):
    compute = Compute()
    unified_results = {
        rule.key: [operator.methodcaller(rule.method, rule, dataframe)(compute)]
        for rule in check.rules
    }

    def _calculate_violations(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 0
            else:
                return nrows
        elif isinstance(result, Number):
            if isinstance(result, complex):
                return result.imag
            else:
                return nrows - result

    def _calculate_pass_rate(result, nrows):
        if isinstance(result, (bool, np.bool_)):
            if result:
                return 1.0
            else:
                return 0.0
        elif isinstance(result, Number):
            if isinstance(result, complex):
                if result.imag > 0:
                    if result.imag > nrows:
                        return nrows / result.imag
                    else:
                        return result.imag / nrows
                else:
                    return 1.0

            else:
                return result / nrows

    def _evaluate_status(pass_rate, pass_threshold):
        if pass_rate >= pass_threshold:
            return "PASS"

        return "FAIL"

    rows = len(dataframe)

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
            "violations": _calculate_violations(first(unified_results[hash_key]), rows),
            "pass_rate": _calculate_pass_rate(first(unified_results[hash_key]), rows),
            "pass_threshold": rule.coverage,
            "status": _evaluate_status(
                _calculate_pass_rate(first(unified_results[hash_key]), rows),
                rule.coverage,
            ),
        }
        for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
    ]
    return daft.from_pylist(computation_basis)
