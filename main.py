from pyspark import SparkConf
from pyspark.sql import SparkSession

from df_io import *
from postprocessing import remove_null_row, remove_null_col, fill_null_col
from tasks import *

conf = SparkConf().setMaster("local[*]").setAppName("Task App").set("spark.driver.memory", "6g")

def get_session() -> SparkSession:
    return (SparkSession
            .builder
            .config(conf=conf)
            .getOrCreate())

if __name__ == "__main__":
    # Getting session
    spark = get_session()

    # Reading dataframes from files
    name_basics = read_name_basics_df(spark)
    title_akas = read_title_akas_df(spark)
    title_basics = read_title_basics_df(spark)
    title_crew = read_title_crew_df(spark)
    title_episode = read_title_episode_df(spark)
    title_principals = read_title_principals_df(spark)
    title_ratings = read_title_ratings_df(spark)

    # Handling null values
    name_basics = remove_null_row(name_basics, "primary_name")
    name_basics = remove_null_col(name_basics, ["death_year", "primary_profession", "known_for_titles"])
    title_akas = remove_null_row(title_akas, "region")
    title_akas = remove_null_col(title_akas, ["language", "types", "attributes", "is_original_title"])
    title_basics = fill_null_col(title_basics, ("is_adult", False))

    # Caching dataframes to speed up task completion
    name_basics = name_basics.cache()
    title_akas = title_akas.cache()
    title_basics = title_basics.cache()
    title_crew = title_crew.cache()
    title_episode = title_episode.cache()
    title_principals = title_principals.cache()
    title_ratings = title_ratings.cache()

    # Completing tasks
    write_request_df("task.1", task_1(title_basics, title_ratings))
    write_request_df("task.2", task_2(title_basics, title_ratings))
    write_request_df("task.3", task_3(title_basics, title_ratings))
    write_request_df("task.4", task_4(spark, name_basics, title_basics, title_principals))
    write_request_df("task.5", task_5(title_basics, title_ratings))
    write_request_df("task.6", task_6(title_episode))
    write_request_df("task.7", task_7(title_principals, title_ratings))
    write_request_df("task.8", task_8(title_akas))
    write_request_df("task.9", task_9(name_basics, title_basics, title_principals, title_ratings))
    write_request_df("task.10", task_10(title_basics, title_ratings))
    write_request_df("task.11", task_11(title_episode))
    write_request_df("task.12", task_12(title_basics, title_principals))
    write_request_df("task.13", task_13(title_basics, title_ratings))
    write_request_df("task.14", task_14(title_basics))
    write_request_df("task.15", task_15(title_basics))
    write_request_df("task.16", task_16(name_basics, title_basics, title_crew, title_ratings))
    write_request_df("task.17", task_17(title_basics))
    write_request_df("task.18", task_18(name_basics, title_basics, title_principals, title_ratings))
    write_request_df("task.19", task_19(title_akas, title_ratings))
    write_request_df("task.20", task_20(title_basics))