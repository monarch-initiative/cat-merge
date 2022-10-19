import pytest
from tests.test_utils import *
from cat_merge.qc_diff_utils import diff_dict
from typing import Dict


@pytest.fixture
def dict1() -> Dict:
    return {'item1': None, 'item7': 'item7', 'item2': 'item2'}


def test_diff_dict_none(dict1, flags):
    assert diff_dict(None, dict1, flags) == {'-item1': None, '-item7': '-item7', '-item2': '-item2'}
    assert diff_dict(dict1, None, flags) == {'+item1': None, '+item7': '+item7', '+item2': '+item2'}
    assert flags["change"] is True


@pytest.fixture
def dict1_copy() -> Dict:
    return {'item1': None, 'item7': 'item7', 'item2': 'item2'}


def test_diff_dict_match(dict1, dict1_copy, flags):
    if flags["show_all"]:
        assert diff_dict(dict1, dict1_copy, flags) == {'item1': None, 'item7': 'item7', 'item2': 'item2'}
        assert diff_dict(dict1_copy, dict1, flags) == {'item1': None, 'item7': 'item7', 'item2': 'item2'}
    else:
        assert diff_dict(dict1, dict1_copy, flags) == dict()
        assert diff_dict(dict1_copy, dict1, flags) == dict()
    assert flags["change"] is False


@pytest.fixture
def empty_dict() -> Dict:
    return dict()


def test_diff_lists_a_empty(empty_dict, dict1, flags):
    assert diff_dict(empty_dict, dict1, flags) == {'-item1': None, '-item7': '-item7', '-item2': '-item2'}
    assert diff_dict(dict1, empty_dict, flags) == {'+item1': None, '+item7': '+item7', '+item2': '+item2'}
    assert flags["change"] is True


@pytest.fixture
def dict2() -> Dict:
    return {'item7': 'item7', 'item1': None, 'item3': 'item3'}


def test_diff_list_no_match(dict1, dict2, flags):
    if flags["show_all"]:
        assert diff_dict(dict1, dict2, flags) == \
               {'item1': None, 'item7': 'item7', '+item2': '+item2', '-item3': '-item3'}
    else:
        assert diff_dict(dict1, dict2, flags) == {'+item2': '+item2', '-item3': '-item3'}
    assert flags["change"] is True
