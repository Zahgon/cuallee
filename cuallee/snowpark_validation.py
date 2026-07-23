import enum
import os
import operator
import snowflake.snowpark.functions as F  # type: ignore
import snowflake.snowpark.types as T  # type: ignore
import snowflake.snowpark.window as W  # type: ignore

from typing import (
    Union,
    Dict,
    Type,
    Callable,
    Iterable,
    Optional,
    Any,
    Tuple,
    List,
)
from dataclasses import dataclass
from snowflake.snowpark import DataFrame, Column, Session, Row
from snowflake.snowpark.session import Session as SnowSession
from toolz import valfilter, first  # type: ignore
from functools import reduce

from cuallee import Check, Rule
import cuallee.utils as cuallee_utils


class ComputeMethod(enum.Enum):
    SELECT = "SELECT"
    TRANSFORM = "TRANSFORM"


@dataclass
class ComputeInstruction:
    predicate: Union[Column, None]
    expression: Column
    compute_method: ComputeMethod

    def __repr__(self):
        return f"ComputeInstruction({self.compute_method})"


class Compute:
    def __init__(self):
        self.compute_instruction = None

    def _sum_predicate_to_integer(self, predicate: Column):
        return F.sum(predicate.cast("integer"))

    def _single_value_rule(
        self,
        column: Union[str, List[str], Tuple[str, str]],
        value: Optional[Union[Tuple[Any], Iterable[Any], Any]],
        operator: Callable,
    ):
        pass

    def _stats_fn_rule(
        self,
        column: Union[str, List[str], Tuple[str, str]],
        value: Optional[Any],
        operator: Callable,
    ):
        pass

    def is_complete(self, rule: Rule):
        """Validation for non-null values in column"""
        predicate = F.col(rule.column).isNotNull()
        self.compute_instruction = ComputeInstruction(
            predicate,
            self._sum_predicate_to_integer(predicate),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def is_empty(self, rule: Rule):
        pass

    def are_complete(self, rule: Rule):
        pass

    def is_unique(self, rule: Rule):
        """Validation for unique values in column"""
        predicate = None  # TODO:  .groupBy("value").count.filter("count > 1")
        self.compute_instruction = ComputeInstruction(
            predicate,
            F.count_distinct(F.col(rule.column)),
            ComputeMethod.SELECT,
        )
        return self.compute_instruction

    def are_unique(self, rule: Rule):
        pass

    def is_greater_than(self, rule: Rule):
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

    def is_contained_in(self, rule: Rule):
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

    def has_correlation(self, rule: Rule):
        pass

    def satisfies(self, rule: Rule):
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


def _field_type_filter(
    dataframe: DataFrame,
    field_type: Union[
        Tuple[Type, Type],
        Type[T.DateType],
        Type[T._NumericType],
        Type[T.TimestampType],
        Type[T.TimeType],
        Type[T.StringType],
    ],
) -> List[str]:
    """Internal method to search for column names based on data type"""
    return [
        f.name for f in dataframe.schema.fields if isinstance(f.datatype, field_type)
    ]




def _compute_select_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught select method"""

    _select = lambda x: x.compute_method.name == ComputeMethod.SELECT.name
    select = valfilter(_select, compute_set)

    if not select:
        return {}

    return (
        dataframe.select(
            *[
                compute_instrunction.expression.alias(hash_key)
                for hash_key, compute_instrunction in select.items()
            ]
        )
        .first()
        .asDict()
    )


def _compute_transform_method(
    compute_set: Dict[str, ComputeInstruction], dataframe: DataFrame
) -> Dict:
    """Compute rules throught spark transform"""

    _transform = lambda x: x.compute_method.name == ComputeMethod.TRANSFORM.name
    transform = valfilter(_transform, compute_set)

    return {
        k: operator.attrgetter(k)(compute_instruction.expression(dataframe, k).first())
        for k, compute_instruction in transform.items()
    }


def _get_snowflake_configurations(snowflake_env: Dict):
    return {k: os.getenv(v, None) for k, v in snowflake_env.items()}  # type: ignore


def numeric_fields(dataframe: DataFrame) -> List[str]:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T._NumericType)


def string_fields(dataframe: DataFrame) -> List[str]:
    """Filter all numeric data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.StringType)


def date_fields(dataframe: DataFrame) -> List[str]:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, (T.DateType, T.TimestampType))


def timestamp_fields(dataframe: DataFrame) -> List[str]:
    """Filter all date data types in data frame and returns field names"""
    return _field_type_filter(dataframe, T.TimestampType)


def compute(rules: Dict[str, Rule]) -> Dict:
    """Create dictionnary containing compute instruction for each rule."""
    return {k: operator.methodcaller(v.method, v)(Compute()) for k, v in rules.items()}


def validate_data_types(rules: List[Rule], dataframe: DataFrame) -> bool:
    """Validate the datatype of each column according to the CheckDataType of the rule's method"""

    rule_match = cuallee_utils.match_columns(rules, dataframe.columns)
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


def summary(check: Check, dataframe: DataFrame) -> DataFrame:
    """Compute all rules in this check for specific data frame"""

    rows = dataframe.count()

    select_result = _compute_select_method(compute(check._rule), dataframe)
    transform_result = _compute_transform_method(compute(check._rule), dataframe)

    unified_results = {**select_result, **transform_result}

    _calculate_violations = lambda result_column: (
        F.when(result_column == "False", F.lit(rows))
        .when(result_column == "True", F.lit(0))
        .when(result_column < 0, F.abs(result_column))
        .otherwise(F.lit(rows) - result_column.cast("long"))
    )

    _calculate_pass_rate = lambda observed_column: (
        F.when(observed_column == "False", F.lit(0.0))
        .when(observed_column == "True", F.lit(1.0))
        .when(
            (observed_column < 0) & (F.abs(observed_column) < rows),
            1 - (F.abs(observed_column) / rows),
        )
        .when((observed_column < 0) & (F.abs(observed_column) == rows), 0.5)
        .when(
            (observed_column < 0) & (F.abs(observed_column) > rows),
            rows / F.abs(observed_column),
        )
        .otherwise(observed_column.cast(T.DoubleType()) / rows)
    )

    _evaluate_status = lambda pass_rate, pass_threshold: (
        F.when(pass_rate >= pass_threshold, F.lit("PASS")).otherwise(F.lit("FAIL"))
    )

    if sessions := valfilter(lambda x: isinstance(x, Session), globals()):
        snowpark = first(sessions.values())
    elif sessions := valfilter(lambda x: isinstance(x, Session), locals()):
        snowpark = first(sessions.values())
    if sessions := valfilter(lambda x: isinstance(x, SnowSession), globals()):
        snowpark = first(sessions.values())
    elif sessions := valfilter(lambda x: isinstance(x, SnowSession), locals()):
        snowpark = first(sessions.values())
    elif check.session:
        snowpark = check.session
    else:

        SNOWFLAKE_ENVIRONMENT = {
            "account": "SF_ACCOUNT",
            "user": "SF_USER",
            "password": "SF_PASSWORD",
            "role": "SF_ROLE",
            "warehouse": "SF_WAREHOUSE",
            "database": "SF_DATABASE",
            "schema": "SF_SCHEMA",
        }

        if not check.config:
            check.config = _get_snowflake_configurations(SNOWFLAKE_ENVIRONMENT)

        assert set(SNOWFLAKE_ENVIRONMENT.keys()).issuperset(
            check.config.keys()
        ), "SnowFlake Environment variables not available in check configuration"

        snowpark = Session.builder.configs(check.config).create()

    computation_basis = snowpark.createDataFrame(
        [
            Row(
                index,
                rule.name,
                str(rule.column),
                str(rule.value),
                str(unified_results[hash_key]),
                rule.coverage,
            )
            for index, (hash_key, rule) in enumerate(check._rule.items(), 1)
        ],
        schema=["id", "rule", "column", "value", "result", "pass_threshold"],
    )

    return computation_basis.select(
        F.col("id"),
        F.lit(check.date.strftime("%Y-%m-%d %H:%M:%S")).alias("timestamp"),
        F.lit(check.name).alias("check"),
        F.lit(check.level.name).alias("level"),
        F.col("column"),
        F.col("rule"),
        F.col("value"),
        F.lit(rows).alias("rows"),
        _calculate_violations(F.col("RESULT")).alias("violations"),
        _calculate_pass_rate(F.col("result")).alias("pass_rate"),
        F.col("pass_threshold").cast(T.DoubleType()).alias("pass_threshold"),
    ).withColumn(
        "status",
        _evaluate_status(F.col("pass_rate"), F.col("pass_threshold")),
    )
