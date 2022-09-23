import pytest
from cat_merge.qc_diff_utils import diff_int


def test_diff_int_exception():
    with pytest.raises(ValueError):
        diff_int(None, None)


@pytest.fixture
def int1() -> int:
    return 90000


def test_diff_int_match(int1):
    assert diff_int(int1, int1) == 90000


def test_diff_int_a_none(int1):
    assert diff_int(None, int1) == "-90000"


def test_diff_int_b_none(int1):
    assert diff_int(int1, None) == "+90000"


@pytest.fixture
def int2() -> int:
    return 89999


def test_diff_int_no_match(int1, int2):
    assert diff_int(int1, int2) == {"change": 1, "new": 90000, "old": 89999}
    assert diff_int(int2, int1) == {"change": -1, "new": 89999, "old": 90000}
