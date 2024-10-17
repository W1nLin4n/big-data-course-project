from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_session() -> SparkSession:
    return (SparkSession
            .builder
            .master("local")
            .appName("task app")
            .config(conf=SparkConf())
            .getOrCreate())

if __name__ == "__main__":
    session = get_session()