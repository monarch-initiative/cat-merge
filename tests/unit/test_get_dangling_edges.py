import pytest
from tests.test_utils import string_df
from cat_merge.merge_utils import get_dangling_edges
from typing import Tuple
from pandas.core.frame import DataFrame


@pytest.fixture
def nodes_and_edges() -> Tuple[DataFrame, DataFrame]:
    nodes = u"""\
    id      category 
    Gene:1  Gene 
    Gene:2  Gene 
    Disease:1  Disease
    Disease:2  Disease
    """

    edges = u"""\
    id      subject object 
    uuid:1  Gene:1  Disease:1 
    uuid:2  Gene:2  Disease:2
    uuid:3  Gene:2  Disease:3
    uuid:4  Gene:3  Disease:1
    uuid:5  Gene:5  Disease:5
    """

    return string_df(nodes), string_df(edges)


def test_get_dangling_edges(nodes_and_edges):
    dangling_edges = get_dangling_edges(edges=nodes_and_edges[1],
                                        nodes=nodes_and_edges[0])

    assert(len(dangling_edges) == 3)

    dangling_edge_ids = list(dangling_edges.id)

    assert('uuid:1' not in dangling_edge_ids)
    assert('uuid:2' not in dangling_edge_ids)
    assert('uuid:3' in dangling_edge_ids)
    assert('uuid:4' in dangling_edge_ids)
    assert('uuid:5' in dangling_edge_ids)

