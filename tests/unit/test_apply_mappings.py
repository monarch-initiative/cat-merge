import pytest
from tests.test_utils import string_df


@pytest.fixture
def edges():
    edges = u"""\
    id      subject  object 
    uuid:1  Gene:1   Disease:1 
    uuid:2  XGene:2  Disease:2
    uuid:3  Gene:2   XDisease:3
    uuid:2  XGene:3  XDisease:4
    """

@pytest.fixture
def mapping():  # owl:sameAs
    mapping = u"""\
    subject_id  predicate_id
    XGene:1     Gene:1
    XGene:2     Gene:2
    XGene:3     Gene:3
    XDisease:1  Disease:1
    XDisease:2  Disease:2
    XDisease:3  Disease:3
    XDisease:4  Disease:4
    """

def test_apply_mappings():
    pass
