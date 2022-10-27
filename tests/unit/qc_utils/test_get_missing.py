import pytest
import pandas as pd
from typing import List
from tests.test_utils import string_df

from cat_merge.qc_utils import get_missing


@pytest.fixture
def edges1() -> pd.DataFrame:
    edges = u"""\
    id            subject              predicate               object
    uuid:ac02d0f0-0d34-11ed-8fbc-f9a92257fb60  WB:WBGene00003401  biolink:has_phenotype  WBPhenotype:0001191
    uuid:ac02d0f1-0d34-11ed-8fbc-f9a92257fb60  WB:WBGene00006887  biolink:has_phenotype  WBPhenotype:0000154
    uuid:ac02d0f2-0d34-11ed-8fbc-f9a92257fb60  WB:WBGene00006887  biolink:has_phenotype  WBPhenotype:0000246
    uuid:ac02d0fd-0d34-11ed-8fbc-f9a92257fb60  WB:WBGene00001612  biolink:has_phenotype  WBPhenotype:0001639
    uuid:ac02d0fe-0d34-11ed-8fbc-f9a92257fb60  WB:WBGene00003001  biolink:has_phenotype  WBPhenotype:0000006
    uuid:ac02d0ff-0d34-11ed-8fbc-f9a92257fb60  WB:WBGene00003001  biolink:has_phenotype  WBPhenotype:0000414
    uuid:ac02d100-0d34-11ed-8fbc-f9a92257fb60  WB:WBGene00003001  biolink:has_phenotype  WBPhenotype:0000414
    uuid:ac02d105-0d34-11ed-8fbc-f9a92257fb60  WB:WBGene00001949  biolink:has_phenotype  WBPhenotype:0000414
    """
    return string_df(edges).astype('string')


@pytest.fixture
def cols1() -> List[str]:
    return ['subject', 'object']


@pytest.fixture
def nodes1() -> pd.Series:
    s = pd.Series(['WB:WBGene00003401', 'WB:WBGene00006887', 'WB:WBGene00001949',
                   'WBPhenotype:0001191', 'WBPhenotype:0000006', 'WBPhenotype:0000414'], dtype='string')
    return s


def test_get_missing(edges1, cols1, nodes1):
    missing = get_missing(edges1, cols1, nodes1)
    assert missing.size == 5
    assert type(missing) == pd.Series
    assert missing.dtype == 'string[python]'
    assert list(missing) == ['WB:WBGene00001612', 'WB:WBGene00003001', 'WBPhenotype:0000154',
                             'WBPhenotype:0000246', 'WBPhenotype:0001639']
