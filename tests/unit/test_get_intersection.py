import pytest
# from tests.test_utils import string_df
# from cat_merge.merge_utils import concat_dataframes
from cat_merge.qc_utils import get_intersection
from typing import List
import pandas as pd


@pytest.fixture
def list1() -> List[str]:
    L = ['this', 'that', 'the']
    return L


@pytest.fixture
def list2() -> List[str]:
    L = ['the', 'other']
    return L


def test_list_intersection_length(list1, list2):
    test_series = get_intersection(list1, list2)
    assert (len(test_series) == 1)


def test_list_intersection_order(list1, list2):
    test_series = get_intersection(list1, list2)
    assert (test_series == ['the'])


@pytest.fixture
def series1() -> pd.Series:
    S = pd.Series(['this', 'that', 'the'])
    return S


@pytest.fixture
def series2() -> pd.Series:
    S = pd.Series(['the', 'other'])
    return S


def test_series_intersection_length(series1, series2):
    test_series = get_intersection(series1, series2)
    assert (len(test_series) == 1)


def test_series_intersection_order(list1, list2):
    test_series = get_intersection(list1, list2)
    assert (test_series[0] == 'the')


# def test_length(dataframes):
#     df = concat_dataframes(list(dataframes))
#     assert(len(df) == 4)
#
#
# def test_columns(dataframes):
#     df = concat_dataframes(list(dataframes))
#     assert(list(df.columns) == ['id', 'category', 'name', 'xrefs', 'synonyms'])
#
#
# @pytest.fixture
# def one_empty_dataframe() -> Tuple[DataFrame, DataFrame]:
#     A = u"""\
#     id      category name
#     Gene:1  Gene     FGF8
#     Gene:2  Gene     PAX2
#     """
#
#     B = u"""\
#     id         category    name
#     """
#
#     return string_df(A), string_df(B)
#
#
# def test_empty_df(one_empty_dataframe):
#     df = concat_dataframes(list(one_empty_dataframe))
#     assert(len(df) == 2)
#
#
# def test_null_dataframe(dataframes):
#     df = concat_dataframes([dataframes[0], None])
#     assert(len(df) == 2)
#     assert(list(df.columns) == ['id', 'category', 'name', 'xrefs'])
