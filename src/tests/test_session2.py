from pyspark.rdd import RDD

from src.session2.rdd import *


def test_rdd_from_list(spark_context):
    result_rdd = rdd_from_list(spark_context, 3)
    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [1, 2, 3]


def test_load_file_to_rdd(spark_context):
    result_rdd = load_file_to_rdd(spark_context, "./data/FL_insurance_sample.csv")
    assert isinstance(result_rdd, RDD)
    assert (
        result_rdd.take(1)[0]
        == "policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity"
    )


def test_op1(spark_context):
    matrix = [[1, 3], [2, 5], [8, 9]]
    matrix_rdd = spark_context.parallelize(matrix)
    result_rdd = op1(spark_context, matrix_rdd)

    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [[2, 0], [4, 2], [16, 6]]


def test_op2(spark_context):
    sentences_rdd = spark_context.parallelize(
        ["Hi everybody", "My name is Fanilo", "and your name is Antoine everybody"]
    )
    result_rdd = op2(spark_context, sentences_rdd)

    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [
        "Hi",
        "everybody",
        "My",
        "name",
        "is",
        "Fanilo",
        "and",
        "your",
        "name",
        "is",
        "Antoine",
        "everybody",
    ]


def test_op3(spark_context):
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    numbers_rdd = spark_context.parallelize(numbers)
    result_rdd = op3(spark_context, numbers_rdd)

    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [1, 3, 5, 7, 9]


def test_op4(spark_context):
    numbers = range(100)
    numbers_rdd = spark_context.parallelize(numbers)
    result = op4(spark_context, numbers_rdd)

    assert result == 166650
