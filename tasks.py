from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window


def task_1(title_basics: DataFrame, title_ratings: DataFrame) -> DataFrame:
    title_basics = title_basics.select(["tconst", "genres", "start_year"])
    title_ratings = title_ratings.select(["tconst", "average_rating"])
    result = (
        title_basics.alias("tb")
        .where(
            F.col("tb.start_year") >= F.year(F.current_date()) - 5
        )
        .join(
            title_ratings.alias("tr"),
            F.col("tb.tconst") == F.col("tr.tconst"),
            "inner"
        )
        .select([
            F.explode(F.col("tb.genres")).alias("genre"),
            F.col("tr.average_rating").alias("rating")
        ])
        .groupBy(F.col("genre"))
        .agg(
            F.avg(F.col("rating")).alias("average_rating")
        )
        .orderBy(F.col("average_rating"), ascending=False)
        .limit(5)
    )
    return result

def task_2(title_basics: DataFrame, title_ratings: DataFrame) -> DataFrame:
    title_basics = title_basics.select(["tconst", "title_type", "start_year"])
    title_ratings = title_ratings.select(["tconst", "num_votes"])
    result = (
        title_basics.alias("tb")
        .where(
            F.col("tb.start_year") >= F.year(F.current_date()) - 5
        )
        .join(
            title_ratings.alias("tr"),
            F.col("tb.tconst") == F.col("tr.tconst"),
            "inner"
        )
        .select([
            F.col("tb.title_type").alias("type"),
            F.col("tr.num_votes").alias("popularity")
        ])
        .groupBy(F.col("type"))
        .agg(
            F.avg(F.col("popularity")).alias("average_popularity")
        )
        .orderBy(F.col("average_popularity"), ascending=False)
    )
    return result

def task_3(title_basics: DataFrame, title_ratings: DataFrame) -> DataFrame:
    title_basics = title_basics.select(["tconst", "is_adult"])
    title_ratings = title_ratings.select(["tconst", "average_rating"])
    result = (
        title_basics.alias("tb")
        .join(
            title_ratings.alias("tr"),
            F.col("tb.tconst") == F.col("tr.tconst"),
            "inner"
        )
        .select([
            F.col("tb.is_adult"),
            F.col("tr.average_rating").alias("rating")
        ])
        .groupBy(F.col("is_adult"))
        .agg(
            F.avg(F.col("rating")).alias("average_rating")
        )
        .orderBy(F.col("average_rating"), ascending=False)
        .limit(5)
    )
    return result

def task_4(spark: SparkSession, name_basics: DataFrame, title_basics: DataFrame, title_principals: DataFrame) -> DataFrame:
    name_basics = name_basics.select(["nconst", "birth_year"]).dropna()
    title_basics = title_basics.select(["tconst", "title_type", "start_year"]).dropna()
    title_principals = title_principals.select(["tconst", "nconst", "category"])
    result_transposed = (
        title_principals.alias("tp")
        .where(
            (F.col("category") == "self") |
            (F.col("category") == "actor") |
            (F.col("category") == "actress")
        )
        .join(
            title_basics.alias("tb")
            .where(F.col("title_type") == "movie"),
            F.col("tp.tconst") == F.col("tb.tconst"),
            "inner"
        )
        .join(
            name_basics.alias("nb"),
            F.col("tp.nconst") == F.col("nb.nconst"),
            "inner"
        )
        .select(["tp.tconst", "tb.start_year", "nb.birth_year"])
        .withColumn(
            "age_group",
            F.when(
                F.col("start_year") - F.col("birth_year") < 13,
                "0-12"
            ).when(
                F.col("start_year") - F.col("birth_year") < 18,
                "13-17"
            ).when(
                F.col("start_year") - F.col("birth_year") < 26,
                "18-25"
            ).when(
                F.col("start_year") - F.col("birth_year") < 40,
                "26-39"
            ).when(
                F.col("start_year") - F.col("birth_year") < 60,
                "40-59"
            ).otherwise("60+")
        )
        .crosstab("tconst", "age_group")
        .agg(
            F.avg(F.col("0-12")).alias("0-12"),
            F.avg(F.col("13-17")).alias("13-17"),
            F.avg(F.col("18-25")).alias("18-25"),
            F.avg(F.col("26-39")).alias("26-39"),
            F.avg(F.col("40-59")).alias("40-59"),
            F.avg(F.col("60+")).alias("60+"),
        )
    )
    result_data = result_transposed.take(1)[0].asDict().values()
    result = spark.createDataFrame(
        [[a, b] for a, b in zip(result_transposed.columns, result_data)],
        ["age_group", "avg_num_actors"]
    )
    return result

def task_5(title_basics: DataFrame, title_ratings: DataFrame) -> DataFrame:
    title_basics = (
        title_basics
        .select(["tconst", "title_type", "primary_title", "start_year"])
        .dropna(how="any", subset=["primary_title", "start_year"])
    )
    title_ratings = title_ratings.select(["tconst", "num_votes"])
    result = (
        title_basics.alias("tb")
        .where(F.col("title_type") == "movie")
        .join(
            title_ratings.alias("tr"),
            F.col("tb.tconst") == F.col("tr.tconst"),
            "inner"
        )
        .select(["tb.primary_title", "tb.start_year", "tr.num_votes"])
        .withColumn(
            "rating",
            F.rank().over(
                Window
                .partitionBy(F.col("start_year"))
                .orderBy(F.desc(F.col("num_votes")))
            )
        )
        .where(F.col("rating") == 1)
        .select(["start_year", "primary_title"])
        .orderBy(F.desc(F.col("start_year")))
    )
    return result

def task_6(title_episode: DataFrame) -> DataFrame:
    title_episode = title_episode.select(["parent_tconst", "season_number"]).dropna()
    result = (
        title_episode
        .groupBy(F.col("parent_tconst"))
        .agg(
            F.max(F.col("season_number")).alias("last_season")
        )
        .agg(
            F.avg(F.col("last_season")).alias("avg_num_seasons")
        )
    )
    return result

def task_7(title_principals: DataFrame, title_ratings: DataFrame) -> DataFrame:
    title_principals = title_principals.select("tconst")
    title_ratings = title_ratings.select(["tconst", "num_votes"])
    result = (
        title_principals.alias("tp")
        .groupBy(F.col("tconst"))
        .agg(
            F.count("*").alias("num_people")
        )
        .join(
            title_ratings.alias("tr"),
            F.col("tp.tconst") == F.col("tr.tconst"),
            "inner"
        )
        .groupBy(F.col("num_people"))
        .agg(
            F.avg(F.col("tr.num_votes")).alias("average_popularity")
        )
        .orderBy(F.col("num_people"))
    )
    return result

def task_8(title_akas: DataFrame) -> DataFrame:
    result = (
        title_akas
        .groupBy(F.col("region"))
        .agg(
            F.count("*").alias("num_translations")
        )
        .orderBy(F.desc(F.col("num_translations")))
        .limit(5)
    )
    return result

def task_9(name_basics: DataFrame, title_basics: DataFrame, title_principals: DataFrame, title_ratings: DataFrame) -> DataFrame:
    name_basics = name_basics.select(["nconst", "primary_name"])
    title_basics = title_basics.select(["tconst", "title_type"])
    title_principals = title_principals.select(["tconst", "nconst", "category"])
    title_ratings = title_ratings.select(["tconst", "average_rating", "num_votes"])
    result = (
        title_principals.alias("tp")
        .where(
            (F.col("category") == "self") |
            (F.col("category") == "actor") |
            (F.col("category") == "actress")
        )
        .select(["tconst", "nconst"])
        .distinct()
        .join(
            title_basics.alias("tb"),
            F.col("tp.tconst") == F.col("tb.tconst"),
            "inner"
        )
        .where(
            (F.col("title_type") == "movie") |
            (F.col("title_type") == "tvSeries")
        )
        .join(
            title_ratings.alias("tr")
            .where(F.col("num_votes") > 100),
            F.col("tp.tconst") == F.col("tr.tconst"),
            "inner"
        )
        .groupBy(F.col("nconst"))
        .agg(
            F.avg(F.col("tr.average_rating")).alias("average_rating"),
            F.count("*").alias("role_count")
        )
        .where(F.col("role_count") > 4)
        .join(
            name_basics.alias("nb"),
            F.col("tp.nconst") == F.col("nb.nconst"),
            "inner"
        )
        .select(["primary_name", "average_rating"])
        .orderBy(F.desc(F.col("average_rating")))
        .limit(5)
    )
    return result

def task_10(title_basics: DataFrame, title_ratings: DataFrame) -> DataFrame:
    title_basics = title_basics.select(["tconst", "title_type", "runtime_minutes"]).dropna(subset=["runtime_minutes"])
    title_ratings = title_ratings.select(["tconst", "average_rating"])
    result = (
        title_basics.alias("tb")
        .where(F.col("title_type") == "movie")
        .join(
            title_ratings.alias("tr"),
            F.col("tb.tconst") == F.col("tr.tconst"),
            "inner"
        )
        .withColumn(
            "runtime_minutes(+0-10)",
            F.floor(F.col("runtime_minutes") / 10) * 10
        )
        .groupBy(F.col("runtime_minutes(+0-10)"))
        .agg(
            F.avg(F.col("average_rating")).alias("average_rating")
        )
        .orderBy(F.col("runtime_minutes(+0-10)"))
    )
    return result

def task_11(title_episode: DataFrame) -> DataFrame:
    title_episode = title_episode.select(["parent_tconst", "season_number", "episode_number"]).dropna()
    result = (
        title_episode
        .groupBy(F.col("parent_tconst"), F.col("season_number"))
        .agg(
            F.count("*").alias("episodes_in_season")
        )
        .agg(
            F.avg(F.col("episodes_in_season")).alias("avg_episodes_in_season")
        )
    )
    return result

def task_12(title_basics: DataFrame, title_principals: DataFrame) -> DataFrame:
    title_basics = title_basics.select(["tconst", "title_type", "start_year"]).dropna()
    title_principals = title_principals.select(["tconst", "nconst", "category"])
    result = (
        title_principals.alias("tp")
        .where(
            (F.col("category") == "self") |
            (F.col("category") == "actor") |
            (F.col("category") == "actress")
        )
        .select(["tconst", "nconst"])
        .distinct()
        .join(
            title_basics.alias("tb")
            .where(F.col("title_type") != "tvSeries"),
            F.col("tp.tconst") == F.col("tb.tconst"),
            "inner"
        )
        .groupBy(F.col("nconst"), F.col("start_year"))
        .agg(
            F.count("*").alias("titles_per_year")
        )
        .agg(
            F.avg("titles_per_year").alias("avg_titles_per_year")
        )
    )
    return result

def task_13(title_basics: DataFrame, title_ratings: DataFrame) -> DataFrame:
    title_basics = title_basics.select(["tconst", "title_type", "start_year"]).dropna()
    title_ratings = title_ratings.select(["tconst", "num_votes"])
    result = (
        title_basics.alias("tb")
        .where(F.col("title_type") == "movie")
        .join(
            title_ratings.alias("tr"),
            F.col("tb.tconst") == F.col("tr.tconst"),
            "inner"
        )
        .groupBy(F.col("start_year").alias("year"))
        .agg(
            F.avg(F.col("num_votes")).alias("average_popularity")
        )
        .orderBy(F.desc(F.col("start_year")))
    )
    return result

def task_14(title_basics: DataFrame) -> DataFrame:
    title_basics = title_basics.select(["title_type", "start_year"]).dropna()
    result = (
        title_basics
        .where(F.col("title_type") == "movie")
        .groupBy(F.col("start_year"))
        .agg(
            F.count("*").alias("num_films")
        )
        .orderBy(F.desc(F.col("start_year")))
    )
    return result

def task_15(title_basics: DataFrame) -> DataFrame:
    title_basics = title_basics.select(F.col("tconst"), F.explode(F.col("genres")).alias("genre"))
    result = (
        title_basics.alias("tb1")
        .join(
            title_basics.alias("tb2"),
            (F.col("tb1.tconst") == F.col("tb2.tconst")) &
            (F.col("tb1.genre") < F.col("tb2.genre")),
            "inner"
        )
        .groupBy(F.col("tb1.genre").alias("genre_1"), F.col("tb2.genre").alias("genre_2"))
        .agg(
            F.count("*").alias("frequency")
        )
        .orderBy(F.desc("frequency"))
        .limit(10)
    )
    return result

def task_16(name_basics: DataFrame, title_basics: DataFrame, title_crew: DataFrame, title_ratings: DataFrame) -> DataFrame:
    name_basics = name_basics.select(["nconst", "primary_name"])
    title_basics = title_basics.select(["tconst", "title_type", "start_year"]).dropna()
    title_crew = title_crew.select(["tconst", "directors"]).dropna()
    title_ratings = title_ratings.select(["tconst", "num_votes"])
    result = (
        title_basics.alias("tb")
        .where(
            F.col("tb.start_year") >= F.year(F.current_date()) - 5
        )
        .where(F.col("title_type") == "movie")
        .join(
            title_ratings.alias("tr"),
            F.col("tb.tconst") == F.col("tr.tconst"),
            "inner"
        )
        .orderBy(F.desc("num_votes"))
        .limit(10)
        .join(
            title_crew.alias("tc"),
            F.col("tb.tconst") == F.col("tc.tconst"),
            "inner"
        )
        .select(F.explode(F.col("directors")).alias("director"))
        .distinct()
        .join(
            name_basics.alias("nb"),
            F.col("director") == F.col("nb.nconst"),
            "inner"
        )
        .select(F.col("primary_name").alias("name"))
        .orderBy(F.col("name"))
    )
    return result

def task_17(title_basics: DataFrame) -> DataFrame:
    title_basics = title_basics.select(["title_type", "primary_title", "start_year", "end_year"]).dropna()
    result = (
        title_basics
        .where(F.col("title_type") == "tvSeries")
        .withColumn(
            "duration_years",
            F.col("end_year") - F.col("start_year")
        )
        .orderBy(F.desc(F.col("duration_years")))
        .select(["primary_title", "duration_years"])
        .limit(5)
    )
    return result

def task_18(name_basics: DataFrame, title_basics: DataFrame, title_principals: DataFrame, title_ratings: DataFrame) -> DataFrame:
    name_basics = name_basics.select(["nconst", "primary_name"])
    title_basics = title_basics.select(["tconst", "primary_title"])
    title_principals = title_principals.select(["tconst", "nconst", "category"])
    title_ratings = title_ratings.select(["tconst", "num_votes"])
    result = (
        title_principals.alias("tp1")
        .where(F.col("category") == "director")
        .join(
            title_ratings.alias("tr1"),
            F.col("tp1.tconst") == F.col("tr1.tconst"),
            "inner"
        )
        .groupBy(F.col("nconst"))
        .agg(
            F.avg(F.col("num_votes")).alias("average_popularity")
        )
        .orderBy(F.desc(F.col("average_popularity")))
        .limit(5)
        .join(
            title_principals.alias("tp2")
            .where(F.col("category") == "director"),
            F.col("tp1.nconst") == F.col("tp2.nconst"),
            "inner"
        )
        .join(
            title_ratings.alias("tr2"),
            F.col("tp2.tconst") == F.col("tr2.tconst"),
            "inner"
        )
        .withColumn(
            "title_rank",
            F.row_number().over(
                Window
                .partitionBy("tp1.nconst")
                .orderBy("num_votes")
            )
        )
        .where(F.col("title_rank") == 1)
        .join(
            name_basics.alias("nb"),
            F.col("tp1.nconst") == F.col("nb.nconst"),
            "inner"
        )
        .join(
            title_basics.alias("tb"),
            F.col("tp2.tconst") == F.col("tb.tconst"),
            "inner"
        )
        .select(
            F.col("primary_name").alias("director_name"),
            F.col("primary_title").alias("title_name")
        )
        .orderBy(F.col("director_name"))
    )
    return result

def task_19(title_akas: DataFrame, title_ratings: DataFrame) -> DataFrame:
    title_akas = title_akas.select(["title_id"])
    title_ratings = title_ratings.select(["tconst", "num_votes"])
    result = (
        title_akas.alias("ta")
        .groupBy(F.col("title_id"))
        .agg(
            F.count("*").alias("num_regions")
        )
        .join(
            title_ratings.alias("tr"),
            F.col("ta.title_id") == F.col("tr.tconst"),
            "inner"
        )
        .groupBy(F.col("num_regions"))
        .agg(
            F.avg("num_votes").alias("average_popularity")
        )
        .orderBy(F.col("num_regions"))
    )
    return result

def task_20(title_basics: DataFrame) -> DataFrame:
    title_basics = title_basics.select(F.col("is_adult"), F.explode(F.col("genres")).alias("genre"))
    result = (
        title_basics
        .where(F.col("is_adult"))
        .groupBy(F.col("genre"))
        .agg(
            F.count("*").alias("num_titles")
        )
        .orderBy(F.desc(F.col("num_titles")))
        .limit(5)
    )
    return result