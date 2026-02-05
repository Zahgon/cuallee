from functools import reduce
import operator
import pandas as pd
import numpy as np
from ....core.rule import Rule
from ..utils import ComputeInstruction, ComputeMethod


def is_complete(rule: Rule):
    """Validation for non-null values in column"""
    predicate = lambda df: df[rule.column].notna()
    return ComputeInstruction(
        predicate,
        lambda df: int(predicate(df).sum()),
        ComputeMethod.OBSERVE,
    )


def are_complete(rule: Rule):
    """Validation for non-null values in a group of columns"""
    predicate = lambda df: df[list(rule.column)].notna().all(axis=1)
    return ComputeInstruction(
        predicate,
        lambda df: int(predicate(df).sum()),
        ComputeMethod.OBSERVE,
    )


def is_empty(rule: Rule):
    """Validation for null values in column"""
    predicate = lambda df: df[rule.column].isna()
    return ComputeInstruction(
        predicate,
        lambda df: int(predicate(df).sum()),
        ComputeMethod.OBSERVE,
    )


def are_empty(rule: Rule):
    """Validation for null values in a group of columns"""
    predicate = lambda df: df[list(rule.column)].isna().all(axis=1)
    return ComputeInstruction(
        predicate,
        lambda df: int(predicate(df).sum()),
        ComputeMethod.OBSERVE,
    )

def is_unique(rule: Rule):
    """Validation for unique values in column"""
    predicate = None
    
    def calculate_unique(df):
        return int(df[rule.column].nunique(dropna=True))

    return ComputeInstruction(
        predicate,
        calculate_unique,
        ComputeMethod.SELECT,
    )


def are_unique(rule: Rule):
    """Validation for unique values in a group of columns"""
    predicate = None
    return ComputeInstruction(
        predicate,
        lambda df: int(df[list(rule.column)].drop_duplicates().shape[0]),
        ComputeMethod.SELECT,
    )

def is_between(rule: Rule):
    """Validation of a column between a range"""
    predicate = lambda df: df[rule.column].between(rule.value[0], rule.value[1], inclusive="both")
    return ComputeInstruction(
        predicate,
        lambda df: int(predicate(df).sum()),
        ComputeMethod.OBSERVE,
    )


def not_between(rule: Rule):
    """Validation of a column outside a range"""
    predicate = lambda df: ~df[rule.column].between(rule.value[0], rule.value[1], inclusive="both")
    return ComputeInstruction(
        predicate,
        lambda df: int(predicate(df).sum()),
        ComputeMethod.OBSERVE,
    )


def is_contained_in(rule: Rule):
    """Validation of column value in set of given values"""
    predicate = lambda df: df[rule.column].isin(rule.value)
    return ComputeInstruction(
        predicate,
        lambda df: int(predicate(df).sum()),
        ComputeMethod.OBSERVE,
    )


def not_contained_in(rule: Rule):
    """Validation of column value not in set of given values"""
    predicate = lambda df: ~df[rule.column].isin(rule.value)
    return ComputeInstruction(
        predicate,
        lambda df: int(predicate(df).sum()),
        ComputeMethod.OBSERVE,
    )


def satisfies(rule: Rule):
    """Validation of a column satisfying a SQL-like predicate / Pandas query"""
    # For Pandas, we can use df.query() or eval() but those return filtered DF or boolean Series.
    # We'll assume the rule value is a string suitable for df.eval() or df.query().
    # Usually `satisfies` takes a condition like "col > 10".
    
    predicate = lambda df: df.eval(rule.value)
    return ComputeInstruction(
        predicate,
        lambda df: int(predicate(df).sum()),
        ComputeMethod.OBSERVE,
    )
