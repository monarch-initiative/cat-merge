import pytest
from tests.test_utils import string_df
from cat_merge.qc_utils import col_to_yaml
from typing import List
import pandas as pd


@pytest.fixture
def series1() -> pd.Series:
    S = pd.Series(['this', 'that', 'the'])
    return S


def test_col_to_yaml_length(series1):
    test_series = col_to_yaml(series1)
    assert (len(test_series) == 3)


def test_col_to_yaml_order(series1):
    test_series = col_to_yaml(series1)
    assert (test_series[0] == 'that')
    assert (test_series[1] == 'the')
    assert (test_series[2] == 'this')
