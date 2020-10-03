from typing import List

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
    # YOUR CODE HERE
    st.info(
        "_Now's a good time to tell you that chaining RDD transformations is possible..._"
    )
    raise NotImplementedError()
