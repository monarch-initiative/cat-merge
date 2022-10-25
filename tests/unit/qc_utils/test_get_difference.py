import pytest
from cat_merge.qc_utils import get_difference
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


def test_list_difference_length(list1, list2):
    test_list = get_difference(list1, list2)
    assert (len(test_list) == 2)


def test_list_difference_order(list1, list2):
    test_list = get_difference(list1, list2)
    assert (test_list == ['that', 'this'])


@pytest.fixture
def series1() -> pd.Series:
    S = pd.Series(['this', 'that', 'the'])
    return S


@pytest.fixture
def series2() -> pd.Series:
    S = pd.Series(['the', 'other'])
    return S


def test_series_difference_length(series1, series2):
    test_series = get_difference(series1, series2)
    assert (len(test_series) == 2)


def test_series_difference_order(series1, series2):
    test_series = get_difference(series1, series2)
    assert (test_series[0] == 'that')
    assert (test_series[1] == 'this')
