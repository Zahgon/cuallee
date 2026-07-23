import operator
from numbers import Number
from typing import Dict, List, Union

import numpy as np
import polars as pl  # type: ignore
from toolz import compose, first  # type: ignore
from toolz.curried import map as map_curried
from cuallee import Check, Rule, CheckStatus
from functools import partial


class Compute:
    @staticmethod
    def _result(series: pl.Series) -> int:
        """It retrieves the sum result of the polar predicate"""
        return compose(operator.itemgetter(0))(series)

    @staticmethod
    def _value(dataframe: pl.DataFrame):
        return compose(
            first,
            first,
            list,
            operator.methodcaller("values"),
            operator.methodcaller("to_dict", as_series=False),
        )(dataframe)

    def is_complete(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate not null"""
        return Compute._result(
            dataframe.select(pl.col(rule.column).is_not_null().cast(pl.Int8))
            .sum()
            .to_series()
        )

    def is_empty(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def are_complete(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_unique(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        """Validate absence of duplicates"""
        expr = pl.col(rule.column)
        flag = False
        if rule.options and isinstance(rule.options, dict):
            flag = rule.options.get("ignore_nulls", False)

        if flag:
            expr = expr.drop_nulls()
            extra = Compute._result(
                dataframe.select(pl.col(rule.column).is_null().cast(pl.Int8)).sum()
            )

        expr = expr.is_unique().cast(pl.Int8)
        base = Compute._result(dataframe.select(expr).sum())

        if flag:
            return base + extra
        else:
            return base

    def are_unique(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_greater_than(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_greater_or_equal_than(
        self, rule: Rule, dataframe: pl.DataFrame
    ) -> Union[bool, int]:
        pass

    def is_less_than(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_less_or_equal_than(
        self, rule: Rule, dataframe: pl.DataFrame
    ) -> Union[bool, int]:
        pass

    def is_equal_than(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_pattern(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_min(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_max(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_std(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_mean(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_sum(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_cardinality(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_infogain(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_between(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_contained_in(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def not_contained_in(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_percentile(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_max_by(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_min_by(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_correlation(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def satisfies(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def has_entropy(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_weekday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_weekend(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_monday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_tuesday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_wednesday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_thursday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_friday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_saturday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_sunday(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_on_schedule(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass

    def is_daily(self, rule: Rule, dataframe: pl.DataFrame) -> complex:
        pass

    def is_inside_interquartile_range(
        self, rule: Rule, dataframe: pl.DataFrame
    ) -> Union[bool, complex]:
        pass

    def has_workflow(self, rule: Rule, dataframe: pl.DataFrame) -> Union[bool, int]:
        pass


def compute(rules: Dict[str, Rule]):
    """Polars computes directly on the predicates"""
    return True


def validate_data_types(rules: List[Rule], dataframe: pl.DataFrame):
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""






    return True


def summary(check: Check, dataframe: pl.DataFrame):
    compute = Compute()
    unified_results = {
        rule.key: [operator.methodcaller(rule.method, rule, dataframe)(compute)]
        for rule in check.rules
    }

    def _calculate_violations(result, nrows):
        if isinstance(result, pl.DataFrame):
            result = first(result.row(0))

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
        if isinstance(result, pl.DataFrame):
            result = first(result.row(0))

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
                        return nrows / (nrows + result.imag)
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
            "column": str(rule.column),
            "rule": rule.name,
            "value": str(rule.value),
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
    pl.Config.set_tbl_cols(12)
    return pl.DataFrame(computation_basis)


def ok(check: Check, dataframe: pl.DataFrame) -> bool:
    """True when all rules in the check pass validation"""

    _all_pass = compose(
        all,
        map_curried(partial(operator.eq, CheckStatus.PASS.value)),
        operator.methodcaller("to_list"),
        operator.methodcaller("to_series"),
        operator.methodcaller("select", "status"),
    )
    return _all_pass(summary(check, dataframe))
