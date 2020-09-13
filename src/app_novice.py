import streamlit as st
from pyspark import SparkConf
from pyspark import SparkContext

from src.tests.test_novice import test_rdd_from_list


def _initialize_spark() -> SparkContext:
    """Create a Spark Context for Streamlit app"""
    conf = SparkConf().setAppName("lecture-lyon2").setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    return sc


def display_about(sc: SparkContext):
    st.title("Spark RDDs")

    st.markdown(
        """
    Apache Spark is a cluster computing engine designed to be fast and general-purpose, 
    making it the ideal choice for processing of large datasets. 
    It answers those two points with efficient data sharing accross computations.

    The past years have seen a major changes in computing systems, 
    as growing data volumes required more and more applications to scale out to large clusters. 
    To solve this problem, a wide range of new programming models have been designed 
    to manage multiple types of computations in a distributed fashion, without having people learn too much about distributed systems. 
    Those programming models would need to deal with parallelism, fault-tolerance and resource sharing for us.

    Google's MapReduce presented a simple and general model for batch processing, which handles faults and parallelism easily. 
    Unfortunately the programming model is not adapted for other types of workloads, 
    and multiple specialized systems were born to answer a specific need in a distributed way.

    * Iterative : Giraph
    * Interactive : Impala, Piccolo, Greenplum
    * Streaming : Storm, Millwheel

    The initial goal of Apache Spark is to try and unify all of the workloads for generality purposes. 
    Matei Zaharia in his PhD dissertation suggests that most of the data flow models that required a 
    specialized system needed efficient data sharing accross computations:

    * Iterative algorithms like PageRank or K-Means need to make multiple passes over the same dataset
    * Interactive data mining often requires running multiple ad-hoc queries on the same subset of data
    * Streaming applications need to maintain and share state over time.

    He then proposes to create a new abstraction that gives its users direct control over data sharing, 
    something that other specialized systems would have built-in for their specific needs. 
    The abstraction is implemented inside a new engine that is today called Apache Spark. 
    The engine makes it possible to support more types of computations than with the original MapReduce in a more efficient way, 
    including interactive queries and stream processing.
    """
    )

    st.markdown(f"Head to http://localhost:4040 to see the Spark interface")


def display_q1(sc: SparkContext):
    test_rdd_from_list(sc)
    st.success("You've solved the exercise!")


def display_q2(sc: SparkContext):
    return


def display_q3(sc: SparkContext):
    return


def display_q4(sc: SparkContext):
    return


def main():
    pages = {
        "About": display_about,
        "Part 1": display_q1,
        "Part 2": display_q2,
        "Part 3": display_q3,
        "Part 4": display_q4,
    }
    st.sidebar.header("Questions")
    sc = _initialize_spark()

    page = st.sidebar.selectbox("Select your question", tuple(pages.keys()))
    pages[page](sc)


if __name__ == "__main__":
    main()
