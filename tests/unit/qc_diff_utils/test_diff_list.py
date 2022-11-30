import pytest
from tests.test_utils import *
from cat_merge.qc_diff_utils import diff_list
from typing import List


@pytest.fixture
def list1() -> List:
    return ['item1', 'item7', 'item2']


def test_diff_list_a_none(list1, flags):
    assert diff_list(None, list1, flags) == ['-item1', '-item7', '-item2']
    assert flags["change"] is True


def test_diff_lists_b_none(list1, flags):
    assert diff_list(list1, None, flags) == ['+item1', '+item7', '+item2']
    assert flags["change"] is True


@pytest.fixture
def list1_copy() -> List:
    return ['item1', 'item7', 'item2']


def test_diff_lists_match(list1, list1_copy, flags):
    if flags["show_all"]:
        assert diff_list(list1, list1_copy, flags) == ['item1', 'item7', 'item2']
    else:
        assert diff_list(list1, list1_copy, flags) == list()
    assert flags["change"] is False


@pytest.fixture
def empty_list() -> List:
    return list()


def test_diff_lists_a_empty(empty_list, list1, flags):
    assert diff_list(empty_list, list1, flags) == ['-item1', '-item7', '-item2']
    assert flags["change"] is True


def test_diff_lists_b_empty(list1, empty_list, flags):
    assert diff_list(list1, empty_list, flags) == ['+item1', '+item7', '+item2']
    assert flags["change"] is True


@pytest.fixture
def list2() -> List:
    return ['item7', 'item1', 'item3']


def test_diff_list_no_match(list1, list2, flags):
    if flags["show_all"]:
        assert diff_list(list1, list2, flags) == ['item1', 'item7', '+item2', '-item3']
    else:
        assert diff_list(list1, list2, flags) == ['+item2', '-item3']
    assert flags["change"] is True
