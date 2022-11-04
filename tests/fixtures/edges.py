import pytest
import pandas as pd
from tests.test_utils import string_df


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


def kg_edges_1() -> pd.DataFrame:
    edges = pd.read_csv("tests/test_data/test_kg_edges.tsv", sep="\t")
    return edges.astype('string')
