from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as t
from pyspark.sql import functions as F

from postprocessing import string_to_array_df, array_to_string_df, int_to_bool_df, bool_to_int_df
from setting import FILE_PATHS, RESULTS_FILE_PATHS


def read_df(spark: SparkSession, schema: t.StructType, path: str) -> DataFrame:
    return spark.read.csv(
        path,
        schema,
        sep="\t",
        encoding="utf-8",
        header=True,
        nullValue=r"\N"
    )

def read_name_basics_df(spark: SparkSession) -> DataFrame:
    schema = t.StructType([
        t.StructField("nconst", t.StringType(), False),
        t.StructField("primaryName", t.StringType(), True),
        t.StructField("birthYear", t.IntegerType(), True),
        t.StructField("deathYear", t.IntegerType(), True),
        t.StructField("primaryProfession", t.StringType(), True),
        t.StructField("knownForTitles", t.StringType(), True)
    ])
    df = read_df(spark, schema, FILE_PATHS["name_basics"])
    df = string_to_array_df(df, ["primaryProfession", "knownForTitles"])
    return df

def read_title_akas_df(spark: SparkSession) -> DataFrame:
    schema = t.StructType([
        t.StructField("titleId", t.StringType(), False),
        t.StructField("ordering", t.IntegerType(), False),
        t.StructField("title", t.StringType(), True),
        t.StructField("region", t.StringType(), True),
        t.StructField("language", t.StringType(), True),
        t.StructField("types", t.StringType(), True),
        t.StructField("attributes", t.StringType(), True),
        t.StructField("isOriginalTitle", t.IntegerType(), True),
    ])
    df = read_df(spark, schema, FILE_PATHS["title_akas"])
    df = string_to_array_df(df, ["types", "attributes"])
    df = int_to_bool_df(df, "isOriginalTitle")
    return df

def read_title_basics_df(spark: SparkSession) -> DataFrame:
    schema = t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("titleType", t.StringType(), True),
        t.StructField("primaryTitle", t.StringType(), True),
        t.StructField("originalTitle", t.StringType(), True),
        t.StructField("isAdult", t.IntegerType(), True),
        t.StructField("startYear", t.IntegerType(), True),
        t.StructField("endYear", t.IntegerType(), True),
        t.StructField("runtimeMinutes", t.IntegerType(), True),
        t.StructField("genres", t.StringType(), True),
    ])
    df = read_df(spark, schema, FILE_PATHS["title_basics"])
    df = string_to_array_df(df, "genres")
    df = int_to_bool_df(df, "isAdult")
    return df

def read_title_crew_df(spark: SparkSession) -> DataFrame:
    schema = t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("directors", t.StringType(), True),
        t.StructField("writers", t.StringType(), True)
    ])
    df = read_df(spark, schema, FILE_PATHS["title_crew"])
    df = string_to_array_df(df, ["directors", "writers"])
    return df

def read_title_episode_df(spark: SparkSession) -> DataFrame:
    schema = t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("parentTconst", t.StringType(), False),
        t.StructField("seasonNumber", t.IntegerType(), True),
        t.StructField("episodeNumber", t.IntegerType(), True)
    ])
    df = read_df(spark, schema, FILE_PATHS["title_episode"])
    return df

def read_title_principals_df(spark: SparkSession) -> DataFrame:
    schema = t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("ordering", t.IntegerType(), False),
        t.StructField("nconst", t.StringType(), False),
        t.StructField("category", t.StringType(), True),
        t.StructField("job", t.StringType(), True),
        t.StructField("characters", t.StringType(), True)
    ])
    df = read_df(spark, schema, FILE_PATHS["title_principals"])
    return df

def read_title_ratings_df(spark: SparkSession) -> DataFrame:
    schema = t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("averageRating", t.FloatType(), True),
        t.StructField("numVotes", t.StringType(), True)
    ])
    df = read_df(spark, schema, FILE_PATHS["title_ratings"])
    return df

def write_df(df: DataFrame, path):
    df.write.csv(
        path,
        mode="overwrite",
        sep="\t",
        header=True,
        nullValue=r"\N",
        encoding="utf-8"
    )

def write_name_basics_df(df: DataFrame):
    df = array_to_string_df(df, ["primaryProfession", "knownForTitles"])
    write_df(df, RESULTS_FILE_PATHS["name_basics"])

def write_title_akas_df(df: DataFrame):
    df = array_to_string_df(df, ["types", "attributes"])
    df = bool_to_int_df(df, "isOriginalTitle")
    write_df(df, RESULTS_FILE_PATHS["title_akas"])

def write_title_basics_df(df: DataFrame):
    df = array_to_string_df(df, "genres")
    df = bool_to_int_df(df, "isAdult")
    write_df(df, RESULTS_FILE_PATHS["title_basics"])

def write_title_crew_df(df: DataFrame):
    df = array_to_string_df(df, ["directors", "writers"])
    write_df(df, RESULTS_FILE_PATHS["title_crew"])

def write_title_episode_df(df: DataFrame):
    write_df(df, RESULTS_FILE_PATHS["title_episode"])

def write_title_principals_df(df: DataFrame):
    write_df(df, RESULTS_FILE_PATHS["title_principals"])

def write_title_ratings_df(df: DataFrame):
    write_df(df, RESULTS_FILE_PATHS["title_ratings"])