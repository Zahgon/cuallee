import enum
import operator
from dataclasses import dataclass
from functools import reduce, partial
from typing import Any, Callable, Dict, List, Tuple, Type, Union

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window as W
from pyspark.sql import Column, DataFrame, Row
from toolz import first, valfilter, last, compose
from toolz.curried import map as map_curried
import cuallee.utils as cuallee_utils
from cuallee import Check, ComputeEngine, Rule, CustomComputeException, CheckStatus

import os

try:
    from pyspark.sql.connect.session import SparkSession as SparkConnectSession

    global spark_connect
    if "SPARK_REMOTE" in os.environ:
        spark_connect = SparkConnectSession.builder.remote(
            os.getenv("SPARK_REMOTE")
        ).getOrCreate()
except (ModuleNotFoundError, ImportError):
    pass


class ComputeMethod(enum.Enum):
    OBSERVE = "OBSERVE"
    SELECT = "SELECT"
    TRANSFORM = "TRANSFORM"


@dataclass
class ComputeInstruction:
    predicate: Union[Column, List[Column], None]
    expression: Union[Callable[[DataFrame, str], Any], Column]
    compute_method: ComputeMethod

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"


class Compute(ComputeEngine):
    def __init__(self):
        """Determine the computational options for Rules"""
        self.compute_instruction: Union[ComputeInstruction, None] = None

    def is_complete(self, rule: Rule):
        """Validation for non-null values in column"""
        predicate = F.col(f"`{rule.column}`").isNotNull().cast("integer")
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.sum(predicate),
            ComputeMethod.OBSERVE,
        )
        return self.compute_instruction

    def is_empty(self, rule: Rule):
        pass

    def are_complete(self, rule: Rule):
        pass

    def is_unique(self, rule: Rule):
        """Validation for unique values in column"""
        predicate = None  # F.count_distinct(F.col(rule.column))
        instruction = "count_distinct"
        if rule.options and (rule.options.get("approximate", False)):
            instruction = f"approx_{instruction}"

        self.compute_instruction = ComputeInstruction(
            predicate,
            operator.methodcaller(instruction, F.col(f"`{rule.column}`"))(F),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def are_unique(self, rule: Rule):
        pass

    def is_greater_than(self, rule: Rule):  # To Do with Predicate
        pass

    def is_greater_or_equal_than(self, rule: Rule):
        pass

    def is_less_than(self, rule: Rule):
        pass

    def is_less_or_equal_than(self, rule: Rule):
        pass

    def is_equal_than(self, rule: Rule):
        pass

    def has_pattern(self, rule: Rule):
        pass

    def has_min(self, rule: Rule):
        pass

    def has_max(self, rule: Rule):
        pass

    def has_mean(self, rule: Rule):
        pass

    def has_std(self, rule: Rule):
        pass

    def has_sum(self, rule: Rule):
        pass

    def has_cardinality(self, rule: Rule):
        pass

    def has_infogain(self, rule: Rule):
        pass

    def is_between(self, rule: Rule):
        pass

    def is_contained_in(self, rule: Rule):  # To Do with Predicate
        pass

    def not_contained_in(self, rule: Rule):
        pass

    def has_percentile(self, rule: Rule):
        pass

    def is_inside_interquartile_range(self, rule: Rule):
        pass

    def has_min_by(self, rule: Rule):
        pass

    def has_max_by(self, rule: Rule):
        pass

    def has_correlation(self, rule: Rule):  # To Do with Predicate
        pass

    def satisfies(self, rule: Rule):  # To Do with Predicate
        pass

    def has_entropy(self, rule: Rule):
        pass

    def is_on_weekday(self, rule: Rule):
        pass

    def is_on_weekend(self, rule: Rule):
        pass

    def is_on_monday(self, rule: Rule):
        pass

    def is_on_tuesday(self, rule: Rule):
        pass

    def is_on_wednesday(self, rule: Rule):
        pass

    def is_on_thursday(self, rule: Rule):
        pass

    def is_on_friday(self, rule: Rule):
        pass

    def is_on_saturday(self, rule: Rule):
        pass

    def is_on_sunday(self, rule: Rule):
        pass

    def is_on_schedule(self, rule: Rule):
        pass

    def is_daily(self, rule: Rule):
        pass

    def has_workflow(self, rule: Rule):
        pass

    def is_custom(self, rule: Rule):
        pass


def _field_type_filter(
    dataframe: DataFrame,
    field_type: Union[
        Type[T.DateType], Type[T.NumericType], Type[T.TimestampType], Type[T.StringType]
    ],
) -> List[str]:
    """Internal method to search for column names based on data type"""
    return set(
        [f.name for f in dataframe.schema.fields if isinstance(f.dataType, field_type)]  # type: ignore
    )


def _replace_observe_compute(computed_expressions: dict) -> dict:
    """Replace observe based check with select"""
    select_only_expressions = {}
    for k, v in computed_expressions.items():
        instruction = v
        if instruction.compute_method.name == ComputeMethod.OBSERVE.name:
            instruction.compute_method = ComputeMethod.SELECT
        select_only_expressions[k] = instruction
    return select_only_expressions


def _compute_observe_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Tuple[int, Dict]:
    """Compute rules throught spark Observation"""

    _observe = lambda x: x.compute_method.name == ComputeMethod.OBSERVE.name
    observe = valfilter(_observe, compute_set)

    if observe:
        from pyspark.sql import Observation

        observation = Observation("observation")

        df_observation = dataframe.observe(
            observation,
            *[
                compute_instruction.expression.alias(hash_key)
                for hash_key, compute_instruction in observe.items()
            ],
        )
        rows = df_observation.count()
        return rows, observation.get
    else:
        rows = dataframe.count()
        return rows, {}


def _compute_select_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught spark select"""

    _select = lambda x: x.compute_method.name == ComputeMethod.SELECT.name
    select = valfilter(_select, compute_set)

    return (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key)
                for hash_key, compute_instrunction in select.items()
            ]
        )
        .first()
        .asDict()  # type: ignore
    )


def _compute_transform_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught spark transform"""

    _transform = lambda x: x.compute_method.name == ComputeMethod.TRANSFORM.name
    transform = valfilter(_transform, compute_set)

    return {
        k: operator.attrgetter(k)(compute_instruction.expression(dataframe, k).first())  # type: ignore
        for k, compute_instruction in transform.items()
    }


def numeric_fields(dataframe: DataFrame) -> List[str]:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.NumericType)


def string_fields(dataframe: DataFrame) -> List[str]:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.StringType)


def date_fields(dataframe: DataFrame) -> List[str]:
    """Filter all date data types in data frame and returns field names"""
    return set(
        [f.name for f in dataframe.schema.fields if isinstance(f.dataType, T.DateType) or isinstance(f.dataType, T.TimestampType)]  # type: ignore
    )


def timestamp_fields(dataframe: DataFrame) -> List[str]:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.TimestampType)


def validate_data_types(rules: List[Rule], dataframe: DataFrame) -> bool:
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""

    rule_match = cuallee_utils.match_columns(
        rules, dataframe.columns, case_sensitive=False
    )
    assert not rule_match, f"Column(s): {rule_match} are not present in dataframe"

    numeric_columns = cuallee_utils.get_rule_columns(
        cuallee_utils.get_numeric_rules(rules)
    )
    numeric_dtypes = numeric_fields(dataframe)
    numeric_match = cuallee_utils.match_data_types(numeric_columns, numeric_dtypes)
    assert not numeric_match, f"Column(s): {numeric_match} are not numeric"

    date_columns = cuallee_utils.get_rule_columns(cuallee_utils.get_date_rules(rules))
    date_dtypes = date_fields(dataframe)
    date_match = cuallee_utils.match_data_types(date_columns, date_dtypes)
    assert not date_match, f"Column(s): {date_match} are not date"

    timestamp_columns = cuallee_utils.get_rule_columns(
        cuallee_utils.get_timestamp_rules(rules)
    )
    timestamp_dtypes = timestamp_fields(dataframe)
    timestamp_match = cuallee_utils.match_data_types(
        timestamp_columns, timestamp_dtypes
    )
    assert not timestamp_match, f"Column(s): {timestamp_match} are not timestamp"

    string_columns = cuallee_utils.get_rule_columns(
        cuallee_utils.get_string_rules(rules)
    )
    string_dtypes = string_fields(dataframe)
    string_match = cuallee_utils.match_data_types(string_columns, string_dtypes)
    assert not string_match, f"Column(s): {string_match} are not string"

    return True


def compute(rules: Dict[str, Rule]) -> Dict:
    """Create dictionnary containing compute instruction for each rule."""
    return {k: operator.methodcaller(v.method, v)(Compute()) for k, v in rules.items()}


def summary(check: Check, dataframe: DataFrame) -> DataFrame:
    """Compute all rules in this check for specific data frame"""
    from pyspark.sql.session import SparkSession

    if "spark_connect" in globals():
        spark = globals()["spark_connect"]
    elif spark_in_session := valfilter(
        lambda x: isinstance(x, SparkSession), globals()
    ):
        spark = first(spark_in_session.values())
    else:
        spark = SparkSession.builder.getOrCreate()

    def _value(x):
        """Removes verbosity for Callable values"""
        if isinstance(x.value, Callable):
            if x.options and isinstance(x.options, dict):
                return x.options.get("custom_value", "f(x)")
        else:
            return str(x.value)

    computed_expressions = compute(check._rule)
    if (int(spark.version.replace(".", "")[:3]) < 330) or (
        "connect" in str(type(spark))
    ):
        computed_expressions = _replace_observe_compute(computed_expressions)

    rows, observation_result = _compute_observe_method(computed_expressions, dataframe)
    select_result = _compute_select_method(computed_expressions, dataframe)
    transform_result = _compute_transform_method(computed_expressions, dataframe)

    unified_results = {**observation_result, **select_result, **transform_result}
    check.rows = rows

    for index, (hash_key, rule) in enumerate(check._rule.items(), 1):
        rule.ordinal = index
        rule.evaluate(unified_results[hash_key], rows)


    cuallee_cloud_flag = os.getenv("CUALLEE_CLOUD_TOKEN", False)
    try:
        if cuallee_cloud_flag:
            from .cloud import publish

            publish(check)
    except ModuleNotFoundError:
        pass

    result = spark.createDataFrame(
        [
            Row(
                rule.ordinal,
                check.date.strftime("%Y-%m-%d %H:%M:%S"),
                check.name,
                check.level.name,
                str(rule.column),
                str(rule.name),
                _value(rule),
                int(check.rows),
                int(rule.violations),
                float(rule.pass_rate),
                float(rule.coverage),
                rule.status,
            )
            for rule in check.rules
        ],
        schema="id int, timestamp string, check string, level string, column string, rule string, value string, rows bigint, violations bigint, pass_rate double, pass_threshold double, status string",
    )

    return result


def ok(check: Check, dataframe: DataFrame) -> bool:
    """True when all rules in the check pass validation"""

    _all_pass = compose(
        all,
        map_curried(partial(operator.eq, CheckStatus.PASS.value)),
        map_curried(operator.attrgetter("status")),
        operator.methodcaller("collect"),
        operator.methodcaller("select", "status"),
    )
    return _all_pass(summary(check, dataframe))
