import pytest
from tests.test_utils import *
from cat_merge.qc_diff_utils import sources_dict
from typing import Dict, List


@pytest.fixture
def empty_dict() -> Dict:
    return {}


def test_sources_dict_empty_dict(empty_dict):
    assert sources_dict(empty_dict) == {}


@pytest.fixture
def empty_list() -> List:
    return list()


def test_sources_dict_empty_list(empty_list):
    assert sources_dict(empty_list) == {}


@pytest.fixture
def list_dict_name() -> List:
    return [{'name': 'name'}]


def test_sources_dict_list_dict_name(list_dict_name):
    assert sources_dict(list_dict_name) == {'name': {'name': 'name'}}


@pytest.fixture
def list_dict_uri() -> List:
    return [{'uri': 'uri'}]


def test_sources_dict_list_dict_uri(list_dict_uri):
    assert sources_dict(list_dict_uri) == {'uri': {'uri': 'uri'}}


@pytest.fixture
def list_dict_name_uri() -> List:
    return [{'name': 'name', 'uri': 'uri'}]


def test_sources_dict_list_dict_name_uri(list_dict_name_uri):
    assert sources_dict(list_dict_name_uri) == {'name': {'name': 'name', 'uri': 'uri'}}


@pytest.fixture
def list_dict_other() -> List:
    return [{'other': 'other'}]


def test_sources_dict_list_dict_other(list_dict_other):
    with pytest.raises(NotImplementedError):
        assert sources_dict(list_dict_other)


@pytest.fixture
def list_wrong_type() -> List:
    return ['']


def test_sources_dict_list_wrong_type(list_wrong_type):
    with pytest.raises(ValueError):
        assert sources_dict(list_wrong_type)


@pytest.fixture
def wrong_type() -> str:
    return ''


def test_sources_dict_wrong_type(wrong_type):
    with pytest.raises(TypeError):
        assert sources_dict(wrong_type)
