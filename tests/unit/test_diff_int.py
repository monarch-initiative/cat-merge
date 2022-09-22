import pytest
from cat_merge.qc_diff_utils import diff_int


@pytest.fixture
def int1() -> int:
    i = 90000
    return i


def test_diff_int_match(int1):
    test_int_match = diff_int(int1, int1)
    assert (test_int_match == 90000)


def test_diff_int_a_none(int1):
    test_int_a_none = diff_int(None, int1)
    result = "-90000"
    assert (test_int_a_none == result)


def test_diff_int_b_none(int1):
    test_int_b_none = diff_int(int1, None)
    result = "+90000"
    assert (test_int_b_none == result)


@pytest.fixture
def int2() -> int:
    i = 89999
    return i


def test_diff_int_no_match(int1, int2):
    test_int_no_match = diff_int(int1, int2)
    result = {
        "change": 1,
        "new": 90000,
        "old": 89999,
    }
    assert (test_int_no_match == result)
