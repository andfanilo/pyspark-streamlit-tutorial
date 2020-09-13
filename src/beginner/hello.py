from typing import List

import streamlit as st


def add(a: int, b: int) -> int:
    """Return a + b"""
    # YOUR CODE HERE

    ### Uncomment the code below and check your application
    # st.info("Hello there! I come from `src/beginner/hello.py`, I'm the `add` method")
    # st.markdown("Feel free to add Streamlit commands to help debug your function")
    # st.write(f"Variable a is {a}, variable b is {b}")
    # st.write(f"Product of numbers is {a * b}")

    raise NotImplementedError()


def squared(numbers: List[int]) -> List[int]:
    """Given a list of numbers, return a list of all those numbers, squared."""
    # YOUR CODE HERE
    raise NotImplementedError()


def is_unique(numbers: List[int]) -> bool:
    """Return True if list of numbers contains only unique numbers, False otherwise."""
    # Here, I will help a bit, use a dictionary to store encountered numbers.
    encountered = {}
    encountered[3] = 1
    for number in numbers:
        if number in encountered:
            encountered[3] += 1
    # YOUR CODE HERE
    raise NotImplementedError()
