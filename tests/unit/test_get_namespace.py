import pytest
from cat_merge.qc_utils import get_namespace
import pandas as pd


@pytest.fixture
def series1() -> pd.Series:
    S = pd.Series(['TEST:TEST-GENE-010000-1', 'TEST2:TEST-GENE-010000-2', 'TEST3:TEST-GENE-010002-1'])
    return S


def test_get_namespace_length(series1):
    test_namespace = get_namespace(series1)
    assert (len(test_namespace) == 3)


