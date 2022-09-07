import pytest
# from tests.test_utils import string_df
# from cat_merge.merge_utils import concat_dataframes
from cat_merge.qc_utils import get_namespace
from typing import List
import pandas as pd


@pytest.fixture
def id_str() -> str:
    return 'TEST:TEST-GENE-010000-1'


def test_get_namespace(id_str):
    test_namespace = get_namespace(id_str)
    assert (test_namespace == 'TEST')
