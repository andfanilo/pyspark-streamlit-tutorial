from pyspark.rdd import RDD

from src.session2.rdd import *
from src.session2.pagerank import *


def test_rdd_from_list(spark_context):
    result_rdd = rdd_from_list(spark_context, 3)
    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [1, 2, 3]


def test_load_file_to_rdd(spark_context):
    result_rdd = load_file_to_rdd(spark_context, "./data/FL_insurance_sample.csv")
    assert isinstance(result_rdd, RDD)
    assert (
        result_rdd.take(1)[0]
        == "policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity"
    )


def test_op1(spark_context):
    matrix = [[1, 3], [2, 5], [8, 9]]
    matrix_rdd = spark_context.parallelize(matrix)
    result_rdd = op1(spark_context, matrix_rdd)

    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [[2, 0], [4, 2], [16, 6]]


def test_op2(spark_context):
    sentences_rdd = spark_context.parallelize(
        ["Hi everybody", "My name is Fanilo", "and your name is Antoine everybody"]
    )
    result_rdd = op2(spark_context, sentences_rdd)

    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [
        "Hi",
        "everybody",
        "My",
        "name",
        "is",
        "Fanilo",
        "and",
        "your",
        "name",
        "is",
        "Antoine",
        "everybody",
    ]


def test_op3(spark_context):
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    numbers_rdd = spark_context.parallelize(numbers)
    result_rdd = op3(spark_context, numbers_rdd)

    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [1, 3, 5, 7, 9]


def test_op4(spark_context):
    numbers = range(100)
    numbers_rdd = spark_context.parallelize(numbers)
    result = op4(spark_context, numbers_rdd)

    assert result == 166650


def test_wordcount(spark_context):
    sentences_rdd = spark_context.parallelize(
        ["Hi everybody", "My name is Fanilo", "and your name is Antoine everybody"]
    )
    result_rdd = wordcount(spark_context, sentences_rdd)

    assert isinstance(result_rdd, RDD)
    assert result_rdd.collect() == [
        ("Hi", 1),
        ("everybody", 2),
        ("My", 1),
        ("name", 2),
        ("is", 2),
        ("Fanilo", 1),
        ("and", 1),
        ("your", 1),
        ("Antoine", 1),
    ]


def test_mean_grade_per_gender(spark_context):
    genders_rdd = spark_context.parallelize(
        [("1", "M"), ("2", "M"), ("3", "F"), ("4", "F"), ("5", "F"), ("6", "M")]
    )
    grades_rdd = spark_context.parallelize(
        [("1", 5), ("2", 12), ("3", 7), ("4", 18), ("5", 9), ("6", 5)]
    )

    result_rdd = mean_grade_per_gender(spark_context, genders_rdd, grades_rdd)
    assert isinstance(result_rdd, RDD)
    result = result_rdd.collectAsMap()
    result["M"] - 7.3 < 0.1
    result["F"] - 11.3 < 0.1


def test_filter_header(spark_context):
    header = "policyID,statecode,county,eq_site_limit,hu_site_limit,fl_site_limit,fr_site_limit,tiv_2011,tiv_2012,eq_site_deductible,hu_site_deductible,fl_site_deductible,fr_site_deductible,point_latitude,point_longitude,line,construction,point_granularity"
    file = load_file_to_rdd(spark_context, "./data/FL_insurance_sample.csv")
    result_rdd = filter_header(spark_context, file)

    assert isinstance(result_rdd, RDD)
    assert file.filter(lambda line: line == header).collect()
    assert not result_rdd.filter(lambda line: line == header).collect()


def test_county_count(spark_context):
    file_rdd = filter_header(
        spark_context, load_file_to_rdd(spark_context, "./data/FL_insurance_sample.csv")
    )
    county_rdd = county_count(spark_context, file_rdd)

    result = county_count(spark_context, county_rdd).collectAsMap()
    assert result["CLAY COUNTY"] == 346


def test_ungroup_input(spark_context):
    system = spark_context.parallelize(
        [("a", ["b", "c", "d"]), ("c", ["b"]), ("b", ["c", "d"]), ("d", ["a", "c"])]
    )
    result = ungroup_input(spark_context, system).collect()
    assert result == [
        ("a", "b"),
        ("a", "c"),
        ("a", "d"),
        ("c", "b"),
        ("b", "c"),
        ("b", "d"),
        ("d", "a"),
        ("d", "c"),
    ]


def test_group_input(spark_context):
    system = spark_context.parallelize(
        [
            ("a", "b"),
            ("a", "c"),
            ("a", "d"),
            ("c", "b"),
            ("b", "c"),
            ("b", "d"),
            ("d", "a"),
            ("d", "c"),
        ]
    )
    result = group_input(spark_context, system).collect()
    assert result == [
        ("a", ["b", "c", "d"]),
        ("c", ["b"]),
        ("b", ["c", "d"]),
        ("d", ["a", "c"]),
    ]


def test_compute_contributions():
    assert compute_contributions(["b", "c", "d"], 1) == [
        ("b", 1 / 3),
        ("c", 1 / 3),
        ("d", 1 / 3),
    ]


def test_generate_contributions(spark_context):
    links = spark_context.parallelize(
        [("a", ["b", "c", "d"]), ("c", ["b"]), ("b", ["c", "d"]), ("d", ["a", "c"])]
    )

    ranks = spark_context.parallelize([("a", 1.0), ("c", 3.0), ("b", 2.0), ("d", 4.0)])

    assert generate_contributions(spark_context, links, ranks).collect() == [
        ("b", 3.0),  # contribution from c
        ("c", 1.0),  # contribution from b
        ("d", 1.0),  # contribution from b
        ("a", 2.0),  # contribution from d
        ("c", 2.0),  # contribution from d
        ("b", 1 / 3),  # contribution from a
        ("c", 1 / 3),  # contribution from a
        ("d", 1 / 3),  # contribution from a
    ]


def test_generate_ranks(spark_context):
    contributions = spark_context.parallelize(
        [
            ("b", 3.0),
            ("c", 1.0),
            ("d", 1.0),
            ("a", 2.0),
            ("c", 2.0),
            ("b", 1 / 3),
            ("c", 1 / 3),
            ("d", 1 / 3),
        ]
    )

    result = generate_ranks(spark_context, contributions, 0.85).collect()

    assert [v for k, v in result if k == "a"][0] == 1.85
    assert [v for k, v in result if k == "b"][0] - 2.98 < 0.1
    assert [v for k, v in result if k == "c"][0] - 2.98 < 0.1
    assert [v for k, v in result if k == "d"][0] - 1.28 < 0.1


def test_main(spark_context):
    def initialize(sc):
        links = sc.parallelize(
            [
                ("a", "b"),
                ("a", "c"),
                ("a", "d"),
                ("c", "b"),
                ("b", "c"),
                ("b", "d"),
                ("d", "a"),
                ("d", "c"),
            ]
        )
        links = group_input(sc, links).cache()
        ranks = links.keys().map(lambda url: (url, 0.25))
        return (links, ranks)

    links, ranks = initialize(spark_context)
    result = main(spark_context, 1, 0.85, links, ranks)

    assert result.to_dict()["a"][1] == 0.25625
    assert result.to_dict()["b"][1] - (0.1 + 1 / 3) < 0.1
    assert result.to_dict()["c"][1] - (0.1 + 1 / 3) < 0.1
    assert result.to_dict()["d"][1] - 0.327083 < 0.1
