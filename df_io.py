from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as t
from pyspark.sql import functions as F

from setting import FILE_PATHS


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
    df = (
        df
        .withColumn(
            "primaryProfession",
            F.split(F.col("primaryProfession"), ",")
        )
        .withColumn(
            "knownForTitles",
            F.split(F.col("knownForTitles"), ",")
        )
    )
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
    df = (
        df
        .withColumn(
            "types",
            F.split(F.col("types"), ",")
        )
        .withColumn(
            "attributes",
            F.split(F.col("attributes"), ",")
        )
        .withColumn(
            "isOriginalTitle",
            F.col("isOriginalTitle") == 1
        )
    )
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
    df = (
        df
        .withColumn(
            "isAdult",
            F.col("isAdult") == 1
        )
        .withColumn(
            "genres",
            F.split(F.col("genres"), ",")
        )
    )
    return df

def read_title_crew_df(spark: SparkSession) -> DataFrame:
    schema = t.StructType([
        t.StructField("tconst", t.StringType(), False),
        t.StructField("directors", t.StringType(), True),
        t.StructField("writers", t.StringType(), True)
    ])
    df = read_df(spark, schema, FILE_PATHS["title_crew"])
    df = (
        df
        .withColumn(
            "directors",
            F.split(F.col("directors"), ",")
        )
        .withColumn(
            "writers",
            F.split(F.col("writers"), ",")
        )
    )
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
