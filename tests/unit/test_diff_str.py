import pytest
from tests.test_diff_utils import *
from cat_merge.qc_diff_utils import diff_str


@pytest.fixture
def str1() -> str:
    return "source1.tsv"


def test_diff_str_match(str1, flags):
    if flags["show_all"]:
        assert diff_str(str1, str1, flags) == "source1.tsv"
    else:
        assert diff_str(str1, str1, flags) is None
    assert flags["change"] is False


def test_diff_str_a_none(str1, flags):
    assert diff_str(None, str1, flags) == "-source1.tsv"
    assert flags["change"] is True


def test_diff_str_b_none(str1, flags):
    assert diff_str(str1, None, flags) == "+source1.tsv"
    assert flags["change"] is True


@pytest.fixture
def str2() -> str:
    return "source2.tsv"


def test_diff_int_no_match(str1, str2, flags):
    assert diff_str(str1, str2, flags) == ["+source1.tsv", "-source2.tsv"]
    assert flags["change"] is True
