import pytest
from cat_merge.qc_diff_utils import diff_type
from cat_merge.qc_diff_utils import CompareBothNoneError, CompareDifferentTypesError, TypeNotImplementedError
from typing import List, Dict


def test_diff_type_exceptions():
    with pytest.raises(CompareBothNoneError) as e_info:
        diff_type(None, None)

    with pytest.raises(CompareDifferentTypesError) as e_info:
        diff_type(str(), int())

    with pytest.raises(TypeNotImplementedError) as e_info:
        diff_type(set(), None)


@pytest.fixture
def str1() -> str:
    return "str1"


@pytest.fixture
def str2() -> str:
    return "str2"


def test_diff_type_str(str1, str2):
    assert diff_type(str1, str1) == "str1"
    assert diff_type(None, str1) == "-str1"
    assert diff_type(str1, None) == "+str1"
    assert diff_type(str1, str2) == ["+str1", "-str2"]


@pytest.fixture
def int1() -> int:
    return 0


@pytest.fixture
def int2() -> int:
    return 10


def test_diff_type_int(int1, int2):
    assert diff_type(int1, int1) == 0
    assert diff_type(None, int1) == "-0"
    assert diff_type(int1, None) == "+0"
    assert diff_type(int1, int2) == {"change": -10, "new": 0, "old": 10}


# @pytest.fixture
# def empty_dict() -> Dict:
#     return dict()
#
#
# @pytest.fixture
# def dict_of_none() -> Dict:
#     return {"one": None}
#
#
# def test_diff_type_dict(empty_dict, dict_of_none):
#     assert diff_type(empty_dict, empty_dict) == dict()
#
#
# def test_diff_type_dict_of_none(dict_of_none):
#     assert diff_type(dict_of_none) == {"one": None}
#
#
# @pytest.fixture
# def dict_of_str() -> Dict:
#     return {"one": ""}
#
#
# def test_diff_type_dict_of_str(dict_of_str):
#     assert diff_type(dict_of_str) == {"one": None}
#
#
# @pytest.fixture
# def dict_of_int() -> Dict:
#     return {"one": 0}
#
#
# def test_diff_type_dict_of_int(dict_of_int):
#     assert diff_type(dict_of_int) == {"one": None}
#
#
# @pytest.fixture
# def dict_of_list() -> Dict:
#     return {"one": []}
#
#
# def test_diff_type_dict_of_list(dict_of_list):
#     assert diff_type(dict_of_list) == {"one": []}
#
#
# @pytest.fixture
# def dict_of_dict() -> Dict:
#     return {"one": {}}
#
#
# def test_diff_type_dict_of_dict(dict_of_dict):
#     assert diff_type(dict_of_dict) == {"one": {}}
#
#


@pytest.fixture
def empty_list() -> List:
    return list()


def test_diff_type_list(empty_list):
    assert diff_type(empty_list, empty_list) == list()
    assert diff_type(None, empty_list) == list()
    assert diff_type(empty_list, None) == list()


# @pytest.fixture
# def list_of_str() -> List:
#     return [""]
#
#
# def test_diff_type_list_of_str(list_of_str):
#     assert diff_type(list_of_str) == []
#
#
# @pytest.fixture
# def list_of_int() -> List:
#     return [0]
#
#
# def test_diff_type_list_of_int(list_of_int):
#     assert diff_type(list_of_int) == []
#
#
# @pytest.fixture
# def list_of_dict() -> List[Dict]:
#     return [{}]
#
#
# def test_diff_type_list_of_dict(list_of_dict):
#     assert diff_type(list_of_dict) == [{}]
#
#
# @pytest.fixture
# def list_of_list() -> List[List]:
#     return [[]]
#
#
# def test_diff_type_list_of_list(list_of_list):
#     assert diff_type(list_of_list) == [[]]
