import pytest
from tests.test_utils import string_df
from cat_merge.merge_utils import clean_nodes
from typing import Tuple
from pandas.core.frame import DataFrame


@pytest.fixture
def nodes() -> Tuple[DataFrame, DataFrame]:
    nodes = u"""\
    id      category xrefs
    Gene:1  Gene 
    Gene:2  Gene 
    Gene:2  Gene
    Gene:2  Gene 
    Gene:3  Gene
    Disease:1  Disease
    Disease:2  Disease
    Disease:3  Disease
    Disease:4  Disease
    """

    return string_df(nodes)


def test_clean_nodes(nodes):
    cleaned_nodes = clean_nodes(nodes)

    assert(len(cleaned_nodes) == 7)

