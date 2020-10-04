from pyspark.sql import DataFrame

from src.session3.sparksql import *
from src.session3.sql_ml import *


def test_create_dataframe(spark_session):
    rdd = spark_session.sparkContext.parallelize(
        [("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e"), ("6", "f")]
    )
    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("letter", StringType(), True),
        ]
    )

    result_df = create_dataframe(spark_session, rdd, schema)
    assert result_df.schema == schema
    assert result_df.rdd.collect() == rdd.collect()


def test_read_csv(spark_session):
    result_df = read_csv(spark_session, "./data/FL_insurance_sample.csv")

    assert result_df.schema == StructType(
        [
            StructField("policyID", IntegerType(), True),
            StructField("statecode", StringType(), True),
            StructField("county", StringType(), True),
            StructField("eq_site_limit", DoubleType(), True),
            StructField("hu_site_limit", DoubleType(), True),
            StructField("fl_site_limit", DoubleType(), True),
            StructField("fr_site_limit", DoubleType(), True),
            StructField("tiv_2011", DoubleType(), True),
            StructField("tiv_2012", DoubleType(), True),
            StructField("eq_site_deductible", DoubleType(), True),
            StructField("hu_site_deductible", DoubleType(), True),
            StructField("fl_site_deductible", DoubleType(), True),
            StructField("fr_site_deductible", IntegerType(), True),
            StructField("point_latitude", DoubleType(), True),
            StructField("point_longitude", DoubleType(), True),
            StructField("line", StringType(), True),
            StructField("construction", StringType(), True),
            StructField("point_granularity", IntegerType(), True),
        ]
    )


def test_mean_grade_per_gender(spark_session):
    genders_rdd = spark_session.sparkContext.parallelize(
        [("1", "M"), ("2", "M"), ("3", "F"), ("4", "F"), ("5", "F"), ("6", "M")]
    )
    grades_rdd = spark_session.sparkContext.parallelize(
        [("1", 5), ("2", 12), ("3", 7), ("4", 18), ("5", 9), ("6", 5)]
    )

    genders_schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("gender", StringType(), True),
        ]
    )
    grades_schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("grade", StringType(), True),
        ]
    )

    genders_df = create_dataframe(spark_session, genders_rdd, genders_schema)
    grades_df = create_dataframe(spark_session, grades_rdd, grades_schema)

    result_df = mean_grade_per_gender(spark_session, genders_df, grades_df).toPandas()
    result_df.columns == ["gender", "grade"]

    assert result_df[result_df["gender"] == "F"].values[0][1] - 11.3 < 0.1
    assert result_df[result_df["gender"] == "M"].values[0][1] - 7.3 < 0.1


def test_count_county(spark_session):
    insurance_df = read_csv(spark_session, "./data/FL_insurance_sample.csv")

    df = count_county(spark_session, insurance_df)
    result = df.set_index("county").to_dict()["count"]

    assert result.get("CLAY COUNTY") == 346
