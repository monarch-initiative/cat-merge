import pytest
from cat_merge.qc_diff_utils import get_empty
from typing import List, Dict


@pytest.fixture
def str1() -> str:
    return ""


def test_get_empty_str(str1):
    assert get_empty(str1) is None


@pytest.fixture
def int1() -> int:
    return 0


def test_get_empty_int(int1):
    assert get_empty(int1) is None


@pytest.fixture
def empty_dict() -> Dict:
    return dict()


def test_get_empty_empty_dict(empty_dict):
    assert get_empty(empty_dict) == dict()


@pytest.fixture
def empty_list() -> List:
    return list()


def test_get_empty_empty_list(empty_list):
    assert get_empty(empty_list) == list()


@pytest.fixture
def dict_of_none() -> Dict:
    return {"one": None}


def test_get_empty_dict_of_none(dict_of_none):
    assert get_empty(dict_of_none) == {"one": None}


@pytest.fixture
def dict_of_str() -> Dict:
    return {"one": ""}


def test_get_empty_dict_of_str(dict_of_str):
    assert get_empty(dict_of_str) == {"one": None}


@pytest.fixture
def dict_of_int() -> Dict:
    return {"one": 0}


def test_get_empty_dict_of_int(dict_of_int):
    assert get_empty(dict_of_int) == {"one": None}


@pytest.fixture
def dict_of_list() -> Dict:
    return {"one": []}


def test_get_empty_dict_of_list(dict_of_list):
    assert get_empty(dict_of_list) == {"one": []}


@pytest.fixture
def dict_of_dict() -> Dict:
    return {"one": {}}


def test_get_empty_dict_of_dict(dict_of_dict):
    assert get_empty(dict_of_dict) == {"one": {}}


@pytest.fixture
def list_of_str() -> List:
    return [""]


def test_get_empty_list_of_str(list_of_str):
    assert get_empty(list_of_str) == []


@pytest.fixture
def list_of_int() -> List:
    return [0]


def test_get_empty_list_of_int(list_of_int):
    assert get_empty(list_of_int) == []


@pytest.fixture
def list_of_dict() -> List[Dict]:
    return [{}]


def test_get_empty_list_of_dict(list_of_dict):
    assert get_empty(list_of_dict) == [{}]


@pytest.fixture
def list_of_list() -> List[List]:
    return [[]]


def test_get_empty_list_of_list(list_of_list):
    assert get_empty(list_of_list) == [[]]
