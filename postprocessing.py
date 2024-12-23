from __future__ import annotations

import re
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def string_to_array_df(df: DataFrame, cols: list[str] | str) -> DataFrame:
    if type(cols) == str:
        cols = [cols]
    for col in cols:
        df = (
            df
            .withColumn(
                col,
                F.split(F.col(col), ",")
            )
        )
    return df

def array_to_string_df(df: DataFrame, cols: list[str] | str) -> DataFrame:
    if type(cols) == str:
        cols = [cols]
    for col in cols:
        df = (
            df
            .withColumn(
                col,
                F.array_join(F.col(col), ",")
            )
        )
    return df

def int_to_bool_df(df: DataFrame, cols: list[str] | str) -> DataFrame:
    if type(cols) == str:
        cols = [cols]
    for col in cols:
        df = (
            df
            .withColumn(
                col,
                F.col(col).cast("boolean")
            )
        )
    return df

def bool_to_int_df(df: DataFrame, cols: list[str] | str) -> DataFrame:
    if type(cols) == str:
        cols = [cols]
    for col in cols:
        df = (
            df
            .withColumn(
                col,
                F.col(col).cast("int")
            )
        )
    return df

def camel_to_snake_case(name: str) -> str:
    return re.compile(r"(?<!^)(?=[A-Z])").sub("_", name).lower()

def snake_to_camel_case(name: str) -> str:
    return "".join([(word.capitalize() if i else word) for i, word in enumerate(name.split("_"))])

def camel_to_snake_case_df(df: DataFrame) -> DataFrame:
    for col in df.columns:
        df = (
            df
            .withColumnRenamed(
                col,
                camel_to_snake_case(col)
            )
        )
    return df

def snake_to_camel_case_df(df: DataFrame) -> DataFrame:
    for col in df.columns:
        df = (
            df
            .withColumnRenamed(
                col,
                snake_to_camel_case(col)
            )
        )
    return df

def remove_null_col(df: DataFrame, cols: list[str] | str) -> DataFrame:
    if type(cols) == str:
        cols = [cols]
    for col in cols:
        df = df.drop(F.col(col))
    return df

def remove_null_row(df: DataFrame, cols: list[str] | str) -> DataFrame:
    if type(cols) == str:
        cols = [cols]
    df = df.dropna(how="any", subset=cols)
    return df

def fill_null_col(df: DataFrame, cols_vals: list[tuple[str, Any]] | tuple[str, Any]) -> DataFrame:
    if type(cols_vals) == tuple:
        cols_vals = [cols_vals]
    for (col, val) in cols_vals:
        df = df.fillna(val, col)
    return df