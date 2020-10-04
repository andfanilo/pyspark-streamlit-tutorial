# Pyspark tutorial (using Streamlit)

Welcome to this set of introductory Pyspark exercises with a Streamlit UI for interactive coding, aimed towards students in Statistics/Business intelligence.

The course comprises of 3 list of exercises:

- `src/session1`: Python warm-up exercises to get you started
- `src/session2`: Low-level Spark exercises using RDDs
- `src/session3`: High-level Spark exercises using the Dataframes API and Machine Learning

Each session is associated with a Streamlit app to help you visualize your progression and guide interactive coding. If you are working on `src/session1` then you should start the Streamlit `app_session.py` script to help you out.

A Streamlit app is structured with unit tests to pass by coding inside the designated methods in `src/session*` folders.

## Prerequisites

- [Anaconda 2019+](https://www.anaconda.com/download/)
- Java 8. You may experience difficulties with Java 9. You can set the `JAVA_HOME` environment variable to point to the Java folder you want to use for the project. You may also install Java JDK 8 **inside** your Anaconda environment with `conda install -c cyclus java-jdk`.
- To edit code, I recommend [Visual Studio Code](https://code.visualstudio.com/), but feel free to use the editor you prefer.

## Installation

We provide you with a `requirements.txt` which is used to download dependencies in a conda environment we will name `pyspark-tutorial`.

First open an Anaconda prompt, then create the environment:

```sh
conda create -n pyspark-tutorial python=3.7
```

Activate the environment, which will change your Python executable to use the environment one.

```sh
conda activate pyspark-tutorial
```

Finally, install the project dependencies in your newly activated environment:

```sh
pip install -r requirements.txt
```

## Run

Make sure you have activated your conda environment in an Anaconda promt. Then run a Streamlit script with:

```sh
streamlit run src/app_*.py
```

Streamlit's app should appear in a new tab in your web browser on http://localhost:8501

Then answer questions in `src/session*/` Python files.

When you are done with the environment, don't forget to deactivate your Anaconda environment : `conda deactivate`
