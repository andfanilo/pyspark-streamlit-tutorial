import streamlit as st
from pyspark import SparkContext
from pyspark.rdd import RDD


def rdd_from_list(sc: SparkContext, n: int) -> RDD:
    """Return a RDD consisting of elements from 1 to n.
    For now we assume we will always get n > 1, no need to test for the exception nor raise an Exception.
    """
    # YOUR CODE HERE
    st.info("_Don't forget you can print any value in the Streamlit app_")
    st.write(range(10))
    raise NotImplementedError()
