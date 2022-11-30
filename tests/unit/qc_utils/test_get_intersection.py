import pytest
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

