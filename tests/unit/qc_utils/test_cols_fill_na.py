import pytest
import pandas as pd
from typing import Dict
from tests.test_utils import string_df

from cat_merge.qc_utils import cols_fill_na


@pytest.fixture
def df1() -> pd.DataFrame:
    df = u"""\
    id      category in_taxon
    Gene:1  Gene 
    Gene:2   
    Gene:3  Gene
    """

    return string_df(df)


@pytest.fixture
def cols_dict_in_taxon() -> Dict:
    return {'in_taxon': "missing taxon"}


def test_cols_fill_na_all_na(df1, cols_dict_in_taxon):
    df = cols_fill_na(df1, cols_dict_in_taxon)
    assert all(df['in_taxon'] == "missing taxon")
    assert list(df['category']) == ['Gene', None, 'Gene']
    assert list(df.columns) == ['id', 'category', 'in_taxon']
    assert len(df.index) == 3


@pytest.fixture
def cols_dict_category() -> Dict:
    return {'category': "missing category"}


def test_cols_fill_na_one_na(df1, cols_dict_category):
    df = cols_fill_na(df1, cols_dict_category)
    assert df['in_taxon'].isna().all()
    assert list(df['category']) == ['Gene', "missing category", 'Gene']
    assert list(df.columns) == ['id', 'category', 'in_taxon']
    assert len(df.index) == 3


@pytest.fixture
def cols_dict_both() -> Dict:
    return {'in_taxon': "missing taxon", 'category': "missing category"}


def test_cols_fill_na_both_cols(df1, cols_dict_both):
    df = cols_fill_na(df1, cols_dict_both)
    assert all(df['in_taxon'] == "missing taxon")
    assert list(df['category']) == ['Gene', "missing category", 'Gene']
    assert list(df.columns) == ['id', 'category', 'in_taxon']
    assert len(df.index) == 3
