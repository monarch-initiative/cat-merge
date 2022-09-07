import pytest
from cat_merge.qc_utils import get_namespace


@pytest.fixture
def id_str() -> str:
    return 'TEST:TEST-GENE-010000-1'


def test_get_namespace(id_str):
    test_namespace = get_namespace(id_str)
    assert (test_namespace == 'TEST')
