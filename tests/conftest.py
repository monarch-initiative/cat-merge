import pytest
from cat_merge.model.merged_kg import MergedKG, MergeQC
from cat_merge.file_utils import read_kg
from tests.fixtures.edges import kg_edges_1, kg_report_edges_1
from tests.fixtures.nodes import kg_nodes_1, kg_report_nodes_1


@pytest.fixture
def kg_1(kg_report_edges_1, kg_report_nodes_1) -> MergedKG:
    kg = MergedKG(nodes=kg_report_nodes_1, edges=kg_report_edges_1)
    return kg


@pytest.fixture
def empty_qc(kg_1) -> MergeQC:
    qc = MergeQC(duplicate_nodes=[], dangling_edges=[], duplicate_edges=[])
    return qc
