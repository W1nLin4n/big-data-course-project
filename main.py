from pyspark import SparkConf
from pyspark.sql import SparkSession

from basic_dfs.basic_df_Hrechka import basic_test_df


def get_session() -> SparkSession:
    return (SparkSession
            .builder
            .master("local")
            .appName("task app")
            .config(conf=SparkConf())
            .getOrCreate())

if __name__ == "__main__":
    session = get_session()
    basic_test_df(session).show()