import numpy as np
import pandas as pd
import streamlit as st
from pyspark import SparkConf
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import SparkSession

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
    st.markdown(
        """
    A `Dataset` is a distributed collection of data which combines the benefits of RDDs (strong typing, ability to use lambda functions) 
    and SparkSQL's optimized execution engine.

    A `DataFrame` is a `Dataset` organized into named columns. 
    It is conceptually equivalent to a table in a relational database, or a data frame in Python/R. 
    Conceptually, a `DataFrame` is a `Dataset` of `Row`s.

    As with RDDs, applications can create DataFrames from an existing RDD, a Hive table or from Spark data sources.
    """
    )
    st.subheader("Question 1 - Convert a RDD of Row to a DataFrame")
    st.markdown(
        """
    Recall from the previous assignment how we used two tables on students : 
    one for students to grades, another one for students to gender. 
    
    Let's create a function which takes a `RDD` of `Row`s and a schema as arguments 
    and generates the corresponding DataFrame.
    
    Edit `create_dataframe` in `src/session3/sparksql.py` to solve the issue.
    """
    )
    test_create_dataframe(spark)
    display_exercise_solved()

    st.subheader("Question 2 - Load a CSV file to a DataFrame")
    st.markdown(
        """
    Let's reload the `FL_insurance_sample.csv` file from last session and freely interact with it.

    Edit `read_csv` in `src/session3/sparksql.py` to solve the issue.
    """
    )
    test_read_csv(spark)
    display_goto_next_section()


def display_q2(spark: SparkSession):
    st.title("Running queries on DataFrames")
    st.subheader("Question 1 - The comeback of 'Mean grades per student'")
    st.markdown(
        """
    Let's generate a Dataframe of the students tables for the incoming questions, 
    using our newly created `create_dataframe` function. 
    """
    )
    with st.beta_expander(
        "The following code is run automatically by the Streamlit app."
    ):
        st.markdown(
            """
        ```python
        genders_rdd = spark.sparkContext.parallelize(
            [("1", "M"), ("2", "M"), ("3", "F"), ("4", "F"), ("5", "F"), ("6", "M")]
        )
        grades_rdd = spark.sparkContext.parallelize(
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

        genders_df = create_dataframe(spark, genders_rdd, genders_schema)
        grades_df = create_dataframe(spark, grades_rdd, grades_schema)
        ```
        """
        )
    with st.beta_expander("There are 2 ways of interacting with DataFrames"):
        st.markdown(
            """
        * DataFrames provide a domain-specific language for structured manipulation :

        ```python
        >> genders_df.filter(genders_df['ID'] > 2)
        +---+------+
        | ID|gender|
        +---+------+
        |  3|     F|
        |  4|     F|
        |  5|     F|
        |  6|     M|
        +---+------+
        ```

        In the more simple cases, you can interact with DataFrames with a syntax close to the Pandas syntax.

        ```python
        >> genders_df[genders_df['ID'] > 2]
        +---+------+
        | ID|gender|
        +---+------+
        |  3|     F|
        |  4|     F|
        |  5|     F|
        |  6|     M|
        +---+------+
        ```

        * The `sql` function of a SparkSession enables to run SQL queries directly on the frame 
        and returns a DataFrame, on which you can queue other computations.
        
        Before doing that, you must create a temporary views for those DataFrames 
        so we can interact with them within the SQL query..

        ```python
        # Register the DataFrame as a SQL temporary view beforehand
        >> genders_df.createOrReplaceTempView('genders')

        # Now use the temporary view inside a SQL query, the compiler will map the name to the actual object
        >> spark.sql('SELECT * FROM genders WHERE ID > 2').show()
        +---+------+
        | ID|gender|
        +---+------+
        |  3|     F|
        |  4|     F|
        |  5|     F|
        |  6|     M|
        +---+------+
        ```

        Don't hesitate to check the [DataFrame Function Reference](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions) 
        for all of the operators you can use on a DataFrame. Use the following cell to experiment :)
        """
        )

    st.markdown(
        """
    ---

    Remember the mean grade per gender question from last assignment ? Remember how unpleasant it was ? 
    Let's do that directly in SparkSQL in the `mean_grade_per_gender` method in `src/session3/sparksql.py`. 
    
    PS : if you are using programmatic SQL interaction, you can define a temporary view of temporary variables. 
    You may want to delete those views at the end of your function with `spark.catalog.dropTempView('your_view')`. 
    """
    )
    test_mean_grade_per_gender(spark)
    display_exercise_solved()

    st.subheader("Question 2 - The comeback of 'counting counties'")
    st.markdown(
        """
    Let's plot the number of different counties in a histogram, like in the previous assignment. 
    To do that, in `count_county` return a Pandas a dataframe which contains, for each county, 
    the number of its occurences in the dataset.

    > Hint: a Spark Dataframe is distributed on a number of workers, so it cannot be plotted as is. 
    > You will need to collect the data you want to plot back in the driver. 
    > The `toPandas` is usable to retrieve a Pandas local Dataframe, 
    > be careful to only use it on small Dataframes !
    """
    )
    test_count_county(spark)
    display_exercise_solved()

    st.markdown(
        """
    A little bonus. Streamlit can display plots directly:
    * Matplotlib: `st.pyplot`
    * Plotly: `st.plotly_chart`
    * Bokeh: `st.bokeh_chart`
    * Altair: `st.altair_chart`

    So as a bonus question, display a bar chart of number of occurrences 
    for each county directly in the app by editing the `bar_chart_county` method.
    """
    )
    bar_chart_county(spark)
    display_goto_next_section()


def display_q3(spark: SparkSession):
    st.title("Machine Learning on DataFrames - Titanic example")
    st.markdown("""
    Following the evolution of Spark, there are two ways to do Machine Learning on Spark :

    * MLlib, or `spark.mllib`, was the first ML library implemented in the core Spark library and runs on RDDs. As of today, the library is in maintenance mode, but as we did for RDDs vs DataFrames, it is important that we cover some aspects of the older library. MLlib is also the only library that supports training models for Spark Streaming. 
    * ML, or `spark.ml` is now the primary ML library on Spark, and runs on DataFrames. Its API is close to those of other mainstream librairies like scikit-learn.

    We will dive into both APIs in this notebook, using the `titanic.csv` file for classification purposes on the `Survived` column.

    _If you need a description of the Titanic dataset, [find it here](https://www.kaggle.com/c/titanic/data)_.
    """)

    

    display_goto_next_section()


def display_q4(spark: SparkSession):
    st.title("Application - Convert one of your Python projects to PySpark")
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
        "4 - Application - Pandas to PySpark": display_q4,
        "Resources": display_resources,
    }
    st.sidebar.header("Questions")
    spark = _initialize_spark()

    page = st.sidebar.selectbox("Select your question", tuple(pages.keys()))
    pages[page](spark)


if __name__ == "__main__":
    main()
