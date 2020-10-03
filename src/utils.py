import streamlit as st


def display_exercise_solved():
    st.success("You've solved the exercise!")


def display_goto_next_section():
    st.balloons()
    st.info("You've finished all the questions. Proceed to the next section :tada:")


def display_goto_next_session():
    st.balloons()
    st.info("You've finished all the exercises. On to the next session :sparkles:")
