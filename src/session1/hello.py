from typing import List

import streamlit as st


def add(a: int, b: int) -> int:
    """Return a + b"""
    # YOUR CODE HERE

    ### Uncomment the code below and check your application
    # st.info("Hello there! I come from `src/session1/hello.py`, I'm the `add` method")
    # st.write("Feel free to add Streamlit commands to help debug your function")
    # st.write(f"Variable a is {a}, variable b is {b}")
    # st.write(f"Product of numbers is {a * b}")

    raise NotImplementedError()


def squared(numbers: List[int]) -> List[int]:
    """Given a list of numbers, return a list of all those numbers, squared."""
    # YOUR CODE HERE
    raise NotImplementedError()


def is_unique(numbers: List[int]) -> bool:
    """Return True if list of numbers contains only unique numbers, False otherwise."""
    # I can suggest using a dictionary to store encountered numbers.
    # Feel free to follow this advice, it doesn't lead to the best solution ;).
    encountered = {}
    encountered[3] = 1
    for number in numbers:
        if number in encountered:
            encountered[3] += 1
    # YOUR CODE HERE
    raise NotImplementedError()
