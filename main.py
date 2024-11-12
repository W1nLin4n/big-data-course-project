from pyspark import SparkConf
from pyspark.sql import SparkSession

from df_io import *
from postprocessing import remove_null_row, remove_null_col, fill_null_col

conf = SparkConf().setMaster("local[*]").setAppName("Task App")

def get_session() -> SparkSession:
    return (SparkSession
            .builder
            .config(conf=conf)
            .getOrCreate())

if __name__ == "__main__":
    spark = get_session()
    name_basics = read_name_basics_df(spark)
    title_akas = read_title_akas_df(spark)
    title_basics = read_title_basics_df(spark)
    title_crew = read_title_crew_df(spark)
    title_episode = read_title_episode_df(spark)
    title_principals = read_title_principals_df(spark)
    title_ratings = read_title_ratings_df(spark)
    name_basics = remove_null_row(name_basics, "primary_name")
    name_basics = remove_null_col(name_basics, ["death_year", "primary_profession", "known_for_titles"])
    title_akas = remove_null_row(title_akas, "region")
    title_akas = remove_null_col(title_akas, ["language", "types", "attributes", "is_original_title"])
    title_basics = fill_null_col(title_basics, ("is_adult", False))