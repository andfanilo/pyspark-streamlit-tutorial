import streamlit as st
from pyspark import SparkContext
from pyspark.rdd import RDD


def rdd_from_list(sc: SparkContext, n: int) -> RDD:
    """Return a RDD consisting of elements from 1 to n.
    For now we assume we will always get n > 1, no need to test for the exception nor raise an Exception.
    """
    # YOUR CODE HERE
    # st.info("_Don't forget you can print any value in the Streamlit app_")
    # st.write(range(10))
    raise NotImplementedError()


def load_file_to_rdd(sc: SparkContext, path: str) -> RDD:
    """Create a RDD by loading an external file. We don't expect any formatting nor processing here.
    You don't need to raise an exception if the file does not exist.
    """
    # YOUR CODE HERE
    raise NotImplementedError()


def op1(sc: SparkContext, mat: RDD) -> RDD:
    """Multiply the first coordinate by 2, remove 3 to the second"""
    # YOUR CODE HERE
    # mat = [[1,3], [2,9]]
    # st.write(sc.parallelize(mat).map(lambda row: row[0]).collect())
    raise NotImplementedError()


def op2(sc: SparkContext, sentences: RDD) -> RDD:
    """Return all words contained in the sentences."""
    # YOUR CODE HERE
    raise NotImplementedError()


def op3(sc: SparkContext, numbers: RDD) -> RDD:
    """Return all numbers contained in the RDD that are odd."""
    # YOUR CODE HERE
    # st.write(sc.parallelize(range(20)).filter(lambda num: num > 5).collect())
    raise NotImplementedError()


def op4(sc: SparkContext, numbers: RDD) -> RDD:
    """Return the sum of all squared odd numbers"""
    # YOUR CODE HERE
    st.info(
        "_Now's a good time to tell you that chaining RDD transformations is possible..._"
    )
    raise NotImplementedError()


def wordcount(sc: SparkContext, sentences: RDD) -> RDD:
    """Given a RDD of sentences, return the wordcount, after splitting sentences per whitespace."""
    # YOUR CODE HERE
    # st.write(sc.parallelize(range(10)).map(lambda num: (num % 2, num)).reduceByKey(lambda x,y: x+y).collect())
    raise NotImplementedError()


def mean_grade_per_gender(sc: SparkContext, genders: RDD, grades: RDD) -> RDD:
    """Given a RDD of studentID to grades and studentID to gender, compute mean grade for each gender returned as paired RDD.
    Assume all studentIDs are present in both RDDs, making inner join possible, no need to check that.
    """
    # YOUR CODE HERE
    raise NotImplementedError()


def filter_header(sc: SparkContext, rdd: RDD) -> RDD:
    """From a RDD of lines from a text file, remove the first line."""
    # YOUR CODE HERE
    raise NotImplementedError()


def county_count(sc: SparkContext, rdd: RDD) -> RDD:
    """Return a RDD of key,value with county as key, count as values"""
    # YOUR CODE HERE
    raise NotImplementedError()


def bar_chart_county(sc: SparkContext) -> None:
    """Display a bar chart for the number of occurences for each county
    with Matplotlib, Bokeh, Plotly or Altair...

    Don't return anything, just display in the function directly.

    Load and process the data by using the methods you defined previously.
    """
    # YOUR CODE HERE
    raise NotImplementedError()
