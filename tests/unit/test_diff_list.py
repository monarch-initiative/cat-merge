import pytest
from cat_merge.qc_diff_utils import diff_lists
from typing import List


@pytest.fixture
def list1() -> List:
    return ['item1', 'item7', 'item2']


def test_diff_list_a_none(list1):
    assert diff_lists(None, list1) == ['-item1', '-item7', '-item2']


def test_diff_lists_b_none(list1):
    assert diff_lists(list1, None) == ['+item1', '+item7', '+item2']


@pytest.fixture
def list1_copy() -> List:
    return ['item1', 'item7', 'item2']


def test_diff_lists_match(list1, list1_copy):
    assert diff_lists(list1, list1_copy) == ['item1', 'item7', 'item2']


@pytest.fixture
def empty_list() -> List:
    return list()


def test_diff_lists_a_empty(empty_list, list1):
    assert diff_lists(empty_list, list1) == ['-item1', '-item7', '-item2']


def test_diff_lists_b_empty(list1, empty_list):
    assert diff_lists(list1, empty_list) == ['+item1', '+item7', '+item2']


@pytest.fixture
def list2() -> List:
    return ['item7', 'item1', 'item3']


def test_diff_int_no_match(list1, list2):
    assert diff_lists(list1, list2) == ['item1', 'item7', '+item2', '-item3']
