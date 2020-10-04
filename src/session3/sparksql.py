import streamlit as st

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def create_dataframe(spark: SparkSession, rdd: RDD, schema: StructType) -> DataFrame:
    """Generate a DataFrame from a RDD of Rows and a schema.
    We assume the RDD is correctly formatted, no need to check for anything.
    """
    # YOUR CODE HERE
    # st.write("Hello world")
    raise NotImplementedError()


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """Create a DataFrame by loading an external csv file.
    We don't expect any formatting nor processing here.
    We assume the file has a header, uses " as double quote and , as delimiter.
    Infer its schema automatically.
    You don't need to raise an exception if the file does not exist or doesn't follow the previous constraints.
    """
    # YOUR CODE HERE
    raise NotImplementedError()


def mean_grade_per_gender(
    spark: SparkSession, genders_df: DataFrame, grades_df: DataFrame
) -> DataFrame:
    """Given a RDD of studentID to grades and studentID to gender,
    compute mean grade for each gender returned as paired RDD.
    Assume all studentIDs are present in both RDDs, making inner join possible, no need to check that.
    Schema of output dataframe should bee gender, mean.
    """
    # YOUR CODE HERE
    # If you want to visualize one of your DataFrames, you can download part of it as Pandas.
    # st.write(genders_df[genders_df['ID'] > 2].toPandas())
    raise NotImplementedError()


def count_county(spark: SparkSession, insurance_df: DataFrame) -> DataFrame:
    """Return a Pandas a dataframe which contains, for each county, the number of its occurences in the dataset.
    Schema of the Dataframe should be ['county', 'count']
    """
    # YOUR CODE HERE
    raise NotImplementedError()


def bar_chart_county(spark: SparkSession) -> None:
    """Display a bar chart for the number of occurences for each county
    with Matplotlib, Bokeh, Plotly or Altair...

    Don't return anything, just display in the function directly

    Load and process the data by using the methods you defined previously.
    """
    # YOUR CODE HERE
    raise NotImplementedError()
