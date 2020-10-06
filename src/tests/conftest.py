import pytest
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_context():
    conf = SparkConf().setAppName("lecture-lyon2").setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    yield sc
    sc.stop()


@pytest.fixture(scope="session")
def spark_session():
    conf = SparkConf().setAppName("lecture-lyon2").setMaster("local")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    yield spark
    spark.stop()

