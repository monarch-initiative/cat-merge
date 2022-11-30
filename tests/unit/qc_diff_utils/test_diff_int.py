import pytest
from tests.test_utils import *
from cat_merge.qc_diff_utils import diff_int


@pytest.fixture
def int1() -> int:
    return 90000


def test_diff_int_match(int1, flags):
    if flags["show_all"]:
        assert diff_int(int1, int1, flags) == 90000
    else:
        assert diff_int(int1, int1, flags) is None
    assert flags["change"] is False


def test_diff_int_a_none(int1, flags):
    assert diff_int(None, int1, flags) == "-90000"
    assert flags["change"] is True


def test_diff_int_b_none(int1, flags):
    assert diff_int(int1, None, flags) == "+90000"
    assert flags["change"] is True


@pytest.fixture
def int2() -> int:
    return 89999


def test_diff_int_no_match(int1, int2, flags):
    assert diff_int(int1, int2, flags) == {"change": 1, "new": 90000, "old": 89999}
    assert diff_int(int2, int1, flags) == {"change": -1, "new": 89999, "old": 90000}
    assert flags["change"] is True
