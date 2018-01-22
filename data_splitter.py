import random
import sys
import shutil
import os
import os.path

import pyspark
import pyspark.sql as sql
from pyspark.sql import SparkSession
from  pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf, locate


def main(input_data_csv, training_data, testing_ids, testing_data):
    session = SparkSession.builder.appName("Data Splitter").config("spark.sql.crossJoin.enabled", True).getOrCreate()

    df = session.read.csv(input_data_csv)
    df.printSchema()

    # 37 31 29 23 19 17 13 11 7 5
    split_seed = 5

    print "Splitting training data with random seed {}".format(split_seed)
    idDF = df.select("_c0").dropDuplicates().toDF("_c0")
    # idDF.show()

    splits = idDF.randomSplit([0.94, 0.06], split_seed)
    trainingIds = splits[0].toDF("_c0")
    testingIds = splits[1].toDF("_c0")

    trainingSet = df.join(trainingIds, df._c0 == trainingIds._c0).select(df._c0, df._c1, df._c2, df._c3, df._c4, df._c5, df._c6)
    testingSet = df.join(testingIds, df._c0 == testingIds._c0).select(df._c0, df._c1, df._c2, df._c3, df._c4, df._c5, df._c6)

    trainingSet.show()

    print "Writing Splitted Training Data CSV File ..."
    trainingSet.coalesce(1).write.csv(training_data)

    testingIdTimeStamp = testingSet.groupby("_c0").agg({"_c1": "min"}).withColumnRenamed("_c0", "id").withColumnRenamed("min(_c1)", "ts")
    testingIdTimeStamp.coalesce(1).write.csv(testing_ids)

    testingIdTs = session.read.csv(testing_ids)
    testingIdTs.printSchema()
    testingIdTs.show()

    testingSampleSet = testingSet.join(testingIdTs, (testingSet._c0 == testingIdTs._c0) & (testingSet._c1 == testingIdTs._c1), "inner")\
                                .select(testingSet._c0, testingSet._c1, testingSet._c2, testingSet._c3,testingSet._c4, testingSet._c5, testingSet._c6)

    testingSampleSet.show()

    print "Writing Splitted Testing Data CSV File ..."
    testingSampleSet.coalesce(1).write.csv(testing_data)

    session.stop()


if __name__ == "__main__":
    input_data_csv = "data/training_data_v2.csv"
    training_data = "data/output/training_data_csv"
    testing_ids = "data/output/testing_ids"
    testing_data = "data/output/testing_data_csv"

    curr_file_dir = os.path.dirname(__file__)
    output_dir = os.path.join(curr_file_dir, "data/output")
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    main(input_data_csv, training_data, testing_ids, testing_data)