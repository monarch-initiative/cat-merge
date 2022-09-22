import pytest
from cat_merge.qc_diff_utils import diff_str


@pytest.fixture
def str1() -> str:
    return "source1.tsv"


def test_diff_str_match(str1):
    assert diff_str(str1, str1) == "source1.tsv"


def test_diff_str_a_none(str1):
    assert diff_str(None, str1) == "-source1.tsv"


def test_diff_str_b_none(str1):
    assert diff_str(str1, None) == "+source1.tsv"


@pytest.fixture
def str2() -> str:
    return "source2.tsv"


def test_diff_int_no_match(str1, str2):
    assert diff_str(str1, str2) == ["+source1.tsv", "-source2.tsv"]
