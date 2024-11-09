from __future__ import annotations

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