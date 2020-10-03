import streamlit as st

from src.tests.test_session1 import test_add
from src.tests.test_session1 import test_is_unique
from src.tests.test_session1 import test_squared
from src.utils import display_exercise_solved
from src.utils import display_goto_next_section
from src.utils import display_goto_next_session


def display_about():
    st.title("Warm up")
    st.markdown(
        """
    In this part, we solve simple Python exercises to warm up and understand the tutorial setup.

    * This is a Streamlit app. It will guide you through the questions in an interactive way.
    _You should not edit it, the code to edit is located in `src/session1/hello.py`._
    * The left sidebar will have you navigate between questions.
    * Each Streamlit app will call a set of unit tests. 
    * Each test calls a unique function in `src/session1/hello.py`.
    * Your goal is to edit each function in `src/session1/hello.py` so unit tests pass.  

    Good luck ! :tada:
    """
    )


def display_q1():
    st.subheader("Question 1 - Sum of two numbers")
    st.markdown(
        """
    Edit the `add` method in `src/session1/hello.py` to return the sum of 2 numbers.

    Ex:
    ```python
    add(1, 2) should be 3
    ```
    """
    )
    test_add()
    display_exercise_solved()
    display_goto_next_section()


def display_q2():
    st.subheader("Question 2 - Square numbers in the list")
    st.markdown(
        """
    Edit the `squared` method in `src/session1/hello.py` to square all elements in a list.
    
    Ex:
    ```python
    squared([1, 2, 3]) should be [1, 4, 9]
    ```
    """
    )
    test_squared()
    display_exercise_solved()
    display_goto_next_section()


def display_q3():
    st.subheader("Question 3 - Are all elements unique ?")
    st.markdown(
        """
    Edit the `is_unique` method in `src/session1/hello.py` to return `True` if all elements are unique
    and `False` otherwise.

    Ex:
    ```python
    is_unique([2, 5, 9, 7]) should be True
    is_unique([2, 5, 5, 7]) should be False
    ```
    """
    )
    test_is_unique()
    display_exercise_solved()
    display_goto_next_session()


def main():
    pages = {
        "Python warm-up": display_about,
        "1 - Sum of two numbers": display_q1,
        "2 - Square numbers in the list": display_q2,
        "3 - Are all elements unique ?": display_q3,
    }
    st.sidebar.header("Questions")
    page = st.sidebar.selectbox("Select your question", tuple(pages.keys()))
    pages[page]()


if __name__ == "__main__":
    main()
