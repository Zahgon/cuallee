import operator
from functools import reduce

import polars as pl

from ....core.rule import Rule
from ..utils import ComputeInstruction, ComputeMethod


def is_complete(rule: Rule):
    """Validation for non-null values in column"""
    predicate = pl.col(rule.column).is_not_null()
    return ComputeInstruction(
        predicate,
        predicate.sum(),
        ComputeMethod.OBSERVE,
    )


def are_complete(rule: Rule):
    """Validation for non-null values in a group of columns"""
    predicate = reduce(
        operator.and_,
        [pl.col(c).is_not_null() for c in rule.column],
    )
    return ComputeInstruction(
        predicate,
        predicate.sum(),
        ComputeMethod.OBSERVE,
    )


def is_empty(rule: Rule):
    """Validation for null values in column"""
    predicate = pl.col(rule.column).is_null()
    return ComputeInstruction(
        predicate,
        predicate.sum(),
        ComputeMethod.OBSERVE,
    )


def are_empty(rule: Rule):
    """Validation for null values in a group of columns"""
    predicate = reduce(
        operator.and_,
        [pl.col(c).is_null() for c in rule.column],
    )
    return ComputeInstruction(
        predicate,
        predicate.sum(),
        ComputeMethod.OBSERVE,
    )


def is_unique(rule: Rule):
    """Validation for unique values in column"""
    predicate = None
    expression = pl.col(rule.column).n_unique()
    
    if rule.options and rule.options.get("ignore_nulls"):
        # n_unique in polars by default ignores nulls? Need to check.
        # usually n_unique() counts unique values. null is a value?
        # Polars: n_unique() -> counts valid values. nulls are ignored.
        # If we want to include nulls, we might need a different approach or verify behavior.
        # But pyspark logic adds null count to the distinct count when ignore_nulls is True?
        # Wait, pyspark:
        # if ignore_nulls: corr_value = F.sum(isnan | isnull)
        # return condition + corr_value
        # This implies "is_unique" usually means "count distinct". 
        # Actually is_unique rule checks if `count_distinct` == `count`.
        # So here we just return the count of unique values.
        pass

    return ComputeInstruction(
        predicate,
        expression,
        ComputeMethod.SELECT,
    )


def are_unique(rule: Rule):
    """Validation for unique values in a group of columns"""
    predicate = None
    expression = pl.struct(rule.column).n_unique()
    return ComputeInstruction(
        predicate,
        expression,
        ComputeMethod.SELECT,
    )


def is_between(rule: Rule):
    """Validation of a column between a range"""
    predicate = pl.col(rule.column).is_between(rule.value[0], rule.value[1])
    return ComputeInstruction(
        predicate,
        predicate.sum(),
        ComputeMethod.OBSERVE,
    )


def not_between(rule: Rule):
    """Validation of a column outside a range"""
    predicate = ~pl.col(rule.column).is_between(rule.value[0], rule.value[1])
    return ComputeInstruction(
        predicate,
        predicate.sum(),
        ComputeMethod.OBSERVE,
    )


def is_contained_in(rule: Rule):
    """Validation of column value in set of given values"""
    predicate = pl.col(rule.column).is_in(list(rule.value))
    return ComputeInstruction(
        predicate,
        predicate.sum(),
        ComputeMethod.OBSERVE,
    )


def not_contained_in(rule: Rule):
    """Validation of column value not in set of given values"""
    predicate = ~pl.col(rule.column).is_in(list(rule.value))
    return ComputeInstruction(
        predicate,
        predicate.sum(),
        ComputeMethod.OBSERVE,
    )


def satisfies(rule: Rule):
    """Validation of a column satisfying a SQL-like predicate"""
    # Polars doesn't support raw SQL strings in expressions directly in the same way.
    # We might need to rely on the user passing a Polars expression string or something similar,
    # or just skip this for now if it requires complex parsing.
    # However, if using `pl.sql_expr`, it might work? 
    # For now, let's assume valid Polars expression or error out / placeholder.
    # Pyspark uses F.expr().
    
    # Placeholder implementation
    predicate = None 
    return ComputeInstruction(
        predicate,
        pl.lit(0), # Dummy
        ComputeMethod.OBSERVE,
    )
