import pyspark.sql.types as t
from pyspark.sql import SparkSession

def basic_test_df(session: SparkSession):
    data = [("Apple", 10., 5.5), ("Banana", 30., 2.4), ("Orange", 25., 3.)]
    schema = t.StructType([
        t.StructField("name", t.StringType(), False),
        t.StructField("price", t.FloatType(), True),
        t.StructField("amount", t.FloatType(), True)
    ])
    df = session.createDataFrame(data, schema)
    return df