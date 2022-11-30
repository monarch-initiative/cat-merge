import pytest
import pandas as pd
from typing import List

from cat_merge.qc_utils import get_missing
from tests.fixtures.edges import edges1


@pytest.fixture
def cols1() -> List[str]:
    return ['subject', 'object']


@pytest.fixture
def node_list1() -> pd.Series:
    s = pd.Series(['WB:WBGene00003401', 'WB:WBGene00006887', 'WB:WBGene00001949',
                   'WBPhenotype:0001191', 'WBPhenotype:0000006', 'WBPhenotype:0000414'], dtype='string')
    return s


def test_get_missing(edges1, cols1, node_list1):
    missing = get_missing(edges1, cols1, node_list1)
    assert missing.size == 5
    assert type(missing) == pd.Series
    assert missing.dtype == 'string[python]'
    assert list(missing) == ['WB:WBGene00001612', 'WB:WBGene00003001', 'WBPhenotype:0000154',
                             'WBPhenotype:0000246', 'WBPhenotype:0001639']
