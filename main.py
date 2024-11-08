from pyspark import SparkConf
from pyspark.sql import SparkSession

from df_io import read_name_basics_df, read_title_akas_df, read_title_basics_df, read_title_crew_df, \
    read_title_episode_df, read_title_principals_df, read_title_ratings_df

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
