from typing import List
from typing import Tuple

import pandas as pd
import streamlit as st
from pyspark import SparkContext
from pyspark.rdd import RDD


def ungroup_input(sc: SparkContext, system: RDD) -> RDD:
    """Generate the websystem as an RDD of tuples (page, neighbor page)
    from a RDD of tuples (page name, list of all neighbors)
    """
    # YOUR CODE HERE
    raise NotImplementedError()


def group_input(sc: SparkContext, system: RDD) -> RDD:
    """Generate the websystem as an RDD of tuples (page, list of neighbors)
    from an RDD of tuples (page, neighbor page)
    """
    # YOUR CODE HERE
    raise NotImplementedError()


def compute_contributions(urls: List[str], rank: float) -> List[Tuple[str, float]]:
    """Calculates URL contributions to the rank of other URLs."""
    # YOUR CODE HERE
    # If you're lost on the input/output
    # consult method test_compute_contributions in src/tests/test_session2.py
    raise NotImplementedError()


def generate_contributions(sc: SparkContext, links: RDD, ranks: RDD) -> RDD:
    """Calculates URL contributions to the rank of other URLs."""
    # YOUR CODE HERE
    # Let me suggest you to peek into test_generate_contributions() in src/tests/test_session2.py
    # if you need a concrete example of the expected output.
    raise NotImplementedError()


def generate_ranks(sc: SparkContext, contributions: RDD, damping: float) -> RDD:
    """Calculates URL contributions to the rank of other URLs."""
    # YOUR CODE HERE
    raise NotImplementedError()


def main(
    sc: SparkContext, iterations: int, damping: float, links: RDD, ranks: RDD
) -> pd.DataFrame:
    """Main logic.

    Return pandas dataframe with appended pageranks for each node in order of iterations.

    Example:
    Index A B C D
    1     1 2 3 4   <-- iteration 1
    2     2 3 4 5   <-- iteration 2
    ...
    """
    columns = ["a", "b", "c", "d"]
    pageranks = {"a": [0.25], "b": [0.25], "c": [0.25], "d": [0.25]}
    for iteration in range(iterations):
        print("At iteration %s" % (iteration + 1))
        # YOUR CODE HERE
        raise NotImplementedError()
    return pd.DataFrame(pageranks, columns=columns)
