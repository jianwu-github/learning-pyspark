from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark dataframe example").getOrCreate()

    swimmers = spark.read.json("data/swimmers.json")

    # Creates a temporary view using the DataFrame
    swimmers.createOrReplaceTempView("swimmers")

    sqlDF = spark.sql("SELECT id, name, age, eye_color FROM swimmers")
    sqlDF.show()


