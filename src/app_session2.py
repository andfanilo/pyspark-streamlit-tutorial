import streamlit as st
from pyspark import SparkConf
from pyspark import SparkContext

from src.tests.test_session2 import test_rdd_from_list


def _initialize_spark() -> SparkContext:
    """Create a Spark Context for Streamlit app"""
    conf = SparkConf().setAppName("lecture-lyon2").setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    return sc


def display_about(sc: SparkContext):
    st.title("Introduction to Spark Resilient Distributed Datasets (RDD)")
    st.markdown(
        """
    ![Spark logo](http://spark.apache.org/images/spark-logo-trademark.png)

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


def display_prerequisites(sc: SparkContext):
    st.title("Initializing Spark")
    st.markdown(
        """
    Before running Spark code, we need to start a SparkContext instance, 
    which connects to a Spark cluster. 
    The following code block starts a local Spark cluster and its associated SparkContext. 
    This is done at the beginning of the Streamlit app so you don't have to think about it.

    ```python
    conf = SparkConf()
        .setAppName("lecture-lyon2")  # name of the Spark application
        .setMaster("local")  # which Spark cluster to connect to. If it's a remote cluster you will need to add additional info there
    sc = SparkContext.getOrCreate(conf=conf)
    ```

    While your SparkContext is running, you can hit http://localhost:4040 
    to get an overview of your Spark local cluster and all operations ongoing. Try it now :tada:!
    """
    )


def display_q1(sc: SparkContext):
    st.title("Creating RDDs")
    if st.checkbox("Introduction", False):
        st.markdown(
            """
        In this section, we are going to introduce Spark's core abstraction for working with data 
        in a distributed and resilient way: the **Resilient Distributed Dataset**, or RDD. 
        Under the hood, Spark automatically performs the distribution of RDDs and its processing around 
        the cluster, so we can focus on our code and not on distributed processing problems, 
        such as the handling of data locality or resiliency in case of node failure.

        A RDD consists of a collection of elements partitioned accross the nodes of a cluster of machines 
        that can be operated on in parallel. 
        In Spark, work is expressed by the creation and transformation of RDDs using Spark operators.
        """
        )
        st.image("./img/spark-rdd.png", use_column_width=True)
        st.markdown(
            """
        _Note_: RDD is the core data structure to Spark, but the style of programming we are studying 
        in this lesson is considered the _lowest-level API_ for Spark. 
        The Spark community is pushing the use of Structured programming with Dataframes/Datasets instead, 
        an optimized interface for working with structured and semi-structured data, 
        which we will learn later. 
        Understanding RDDs is still important because it teaches you how Spark works under the hood 
        and will serve you to understand and optimize your application when deployed into production.

        There are two ways to create RDDs: parallelizing an existing collection in your driver program, 
        or referencing a dataset in an external storage system, 
        such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
        """
        )

    st.subheader("Question 1 - From Python collection to RDD")
    st.markdown(
        """
    Edit the `rdd_from_list` method in `src/session2/rdd.py` 
    to generate a Python list and transform it into a Spark RDD.

    Ex:
    ```python
    rdd_from_list([1, 2, 3]) should be a RDD with values [1, 2, 3]
    ```
    """
    )
    test_rdd_from_list(sc)
    st.success("You've solved the exercise!")


def display_q2(sc: SparkContext):
    return


def display_q3(sc: SparkContext):
    return


def display_q4(sc: SparkContext):
    return


def display_pagerank(sc: SparkContext):
    return


def display_resources(sc: SparkContext):
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
        "Introduction to RDDs": display_about,
        "Prerequisites - Spark Initialization": display_prerequisites,
        "1 - Building your first RDDs": display_q1,
        "2 - Operations on RDDs": display_q2,
        "3 - Using Key-value RDDs": display_q3,
        "4 - Manipulating a CSV file": display_q4,
        "5 - Application to Pagerank": display_pagerank,
        "Resources": display_resources,
    }
    st.sidebar.header("Questions")
    sc = _initialize_spark()

    page = st.sidebar.selectbox("Select your question", tuple(pages.keys()))
    pages[page](sc)


if __name__ == "__main__":
    main()
