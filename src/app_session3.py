import numpy as np
import pandas as pd
import streamlit as st
from pyspark import SparkConf
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from src.tests.test_session3 import *
from src.utils import display_exercise_solved
from src.utils import display_goto_next_section
from src.utils import display_goto_next_session


def _initialize_spark() -> SparkSession:
    """Create a Spark Session for Streamlit app"""
    conf = SparkConf().setAppName("lecture-lyon2").setMaster("local")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


def display_about(spark: SparkSession):
    st.title("Introduction to Spark Dataframes")
    st.markdown(
        """
    We introduce SparkSQL, Spark's interface for working with structured data. 
    From Spark 2.0 and forward, this is the preferred way of implementing Spark code, 
    as it contains all of the latest optimisations.

    PySpark benefits a lot from SparkSQL, as there is performance parity between Scala, Java, Python 
    and R interfaces for SparkSQL which use the same optimizer underneath to build the corresponding
    RDD transformations. 
    """
    )
    st.image("./img/catalyst.png", use_column_width=True)


def display_prerequisites(spark: SparkSession):
    st.title("Initializing Spark")
    st.markdown(
        """
    Before running Spark code, we need to start a SparkSession instance, 
    which connects to a Spark cluster. 
    The following code block starts a local Spark cluster and its associated SparkSession. 
    This is done at the beginning of the Streamlit app so you don't have to think about it.

    ```python
    conf = SparkConf()
        .setAppName('lecture-lyon2')  # name of the Spark application
        .setMaster('local')   # which Spark cluster to connect to. If it's a remote cluster you will need to add additional info there
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark
    ```

    While your SparkSession is running, you can hit http://localhost:4040 
    to get an overview of your Spark local cluster and all operations ongoing. Try it now :tada:!
    """
    )


def display_q1(spark: SparkSession):
    st.title("Building your first DataFrames")
    display_goto_next_section()


def display_q2(spark: SparkSession):
    st.title("Running queries on DataFrames")
    display_goto_next_section()


def display_q3(spark: SparkSession):
    st.title("Machine Learning on DataFrames - Titanic example")
    display_goto_next_section()


def display_q4(spark: SparkSession):
    st.title("Application - ")
    display_goto_next_session()


def display_resources(spark: SparkSession):
    st.title("Resources")
    st.markdown(
        """
    List of resources to help you:

    - [Pyspark lecture](https://andfanilo.github.io/pyspark-interactive-lecture/#/)
    - [Spark docs page](https://spark.apache.org/docs/latest/)
    - [PySpark API](https://spark.apache.org/docs/latest/api/python/index.html)

    """
    )


def main():
    pages = {
        "Introduction to Spark Dataframes": display_about,
        "Prerequisites - Spark Initialization": display_prerequisites,
        "1 - Building your first DataFrames": display_q1,
        "2 - Running queries on DataFrames": display_q2,
        "3 - Machine Learning on DataFrames": display_q3,
        "4 - Application - ": display_q4,
        "Resources": display_resources,
    }
    st.sidebar.header("Questions")
    sc = _initialize_spark()

    page = st.sidebar.selectbox("Select your question", tuple(pages.keys()))
    pages[page](sc)


if __name__ == "__main__":
    main()
