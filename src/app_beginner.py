import streamlit as st

from src.tests.test_beginner import test_add
from src.tests.test_beginner import test_is_unique
from src.tests.test_beginner import test_squared


def display_about():
    st.title("Warm up")
    st.markdown(
        """
    In this part, we solve simple Python exercises to warm up and understand the tutorial setup.

    * This is a Streamlit app. It will guide you through the questions in an interactive way.
    _You should not edit it._
    * The left sidebar will have you navigate between questions.
    * Each Streamlit app will call a set of unit tests. 
    * Each test calls a unique function in `src/beginner/hello.py`.
    * Your goal is to edit each function so unit tests don't err.  

    Good luck ! :tada:
    """
    )


def display_q1():
    st.subheader("Question 1 - Sum of two numbers")
    st.markdown("Edit the `add` method in `src/beginner/hello.py`")
    test_add()
    st.success("You've solved the exercise!")


def display_q2():
    st.subheader("Question 2 - Square numbers in the list")
    st.markdown("Edit the `squared` method in `src/beginner/hello.py`")
    test_squared()
    st.success("You've solved the exercise!")


def display_q3():
    st.subheader("Question 3 - Are all elements unique ?")
    st.markdown("Edit the `is_unique` method in `src/beginner/hello.py`")
    test_is_unique()
    st.success("You've solved the exercise!")


def main():
    pages = {
        "About": display_about,
        "Question 1": display_q1,
        "Question 2": display_q2,
        "Question 3": display_q3,
    }
    st.sidebar.header("Questions")
    page = st.sidebar.selectbox("Select your question", tuple(pages.keys()))
    pages[page]()


if __name__ == "__main__":
    main()
