import pytest
from tests.test_utils import string_df
from cat_merge.merge_utils import concat_dataframes
from typing import Tuple
from pandas.core.frame import DataFrame


@pytest.fixture
def dataframes() -> Tuple[DataFrame, DataFrame]:
    A = u"""\
    id      category name xrefs
    Gene:1  Gene     FGF8 SomeGene:3
    Gene:2  Gene     PAX2 SomeGene:4
    """

    B = u"""\
    id         category    name      synonyms
    Anatomy:1  Anatomy     tentacle  sticky-grabber
    Anatomy:2  Anatomy     blowhole  whale-nose
    """

    return string_df(A), string_df(B)


def test_length(dataframes):
    df = concat_dataframes(list(dataframes))
    assert(len(df) == 4)


def test_columns(dataframes):
    df = concat_dataframes(list(dataframes))
    assert(df.index.name == 'id')
    assert(list(df.columns) == ['category', 'name', 'xrefs', 'synonyms'])


@pytest.fixture
def one_empty_dataframe() -> Tuple[DataFrame, DataFrame]:
    A = u"""\
    id      category name
    Gene:1  Gene     FGF8
    Gene:2  Gene     PAX2 
    """

    B = u"""\
    id         category    name 
    """

    return string_df(A), string_df(B)


def test_empty_df(one_empty_dataframe):
    df = concat_dataframes(list(one_empty_dataframe))
    assert(len(df) == 2)


def test_null_dataframe(dataframes):
    df = concat_dataframes([dataframes[0], None])
    assert(len(df) == 2)
    assert(df.index.name == 'id')
    assert(list(df.columns) == ['category', 'name', 'xrefs'])
