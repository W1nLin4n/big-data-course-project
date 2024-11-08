from pyspark import SparkConf
from pyspark.sql import SparkSession

from df_io import *

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
    write_name_basics_df(name_basics)
    write_title_akas_df(title_akas)
    write_title_basics_df(title_basics)
    write_title_crew_df(title_crew)
    write_title_episode_df(title_episode)
    write_title_principals_df(title_principals)
    write_title_ratings_df(title_ratings)