import pytest
import pandas as pd
from cat_merge.qc_utils import cols_fill_na


@pytest.fixture
def kg_nodes_1() -> pd.DataFrame:
    nodes = pd.read_csv("tests/test_data/test_kg_nodes.tsv", sep="\t")
    return nodes.astype('string')


@pytest.fixture
def kg_report_nodes_1(kg_nodes_1) -> pd.DataFrame:
    nodes = cols_fill_na(kg_nodes_1, {'in_taxon': 'missing taxon', 'category': 'missing category'})
    return nodes
