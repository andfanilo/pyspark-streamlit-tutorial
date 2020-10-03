from pyspark.rdd import RDD

from src.session2.rdd import rdd_from_list


def test_rdd_from_list(spark_context):
    result_rdd = rdd_from_list(spark_context, 3)
    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [1, 2, 3]
