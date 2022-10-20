from tests.test_utils import *
from cat_merge.qc_diff_utils import diff_type
from typing import List, Dict


@pytest.fixture
def str1() -> str:
    return "str1"


@pytest.fixture
def str2() -> str:
    return "str2"


def test_diff_type_str(str1, str2, flags):
    if flags["show_all"]:
        assert diff_type(str1, str1, flags) == "str1"
    else:
        assert diff_type(str1, str1, flags) is None
    assert flags['change'] is False

    assert diff_type(None, str1, flags) == "-str1"
    assert flags["change"] is True

    flags['change'] = False
    assert diff_type(str1, None, flags) == "+str1"
    assert flags["change"] is True

    flags['change'] = False
    assert diff_type(str1, str2, flags) == ["+str1", "-str2"]
    assert flags["change"] is True


@pytest.fixture
def int1() -> int:
    return 0


@pytest.fixture
def int2() -> int:
    return 10


def test_diff_type_int(int1, int2, flags):
    if flags["show_all"]:
        assert diff_type(int1, int1, flags) == 0
    else:
        assert diff_type(int1, int1, flags) is None
        assert flags["change"] is False

    assert diff_type(None, int1, flags) == "-0"
    assert flags["change"] is True

    flags['change'] = False
    assert diff_type(int1, None, flags) == "+0"
    assert flags["change"] is True

    flags['change'] = False
    assert diff_type(int1, int2, flags) == {"change": -10, "new": 0, "old": 10}
    assert flags["change"] is True


@pytest.fixture
def empty_dict() -> Dict:
    return dict()


def test_diff_type_dict_empty(empty_dict, flags):
    assert diff_type(None, empty_dict, flags) == dict()
    assert flags['change'] is True
    flags["change"] = False

    assert diff_type(empty_dict, None, flags) == dict()
    assert flags['change'] is True
    flags["change"] = False

    assert diff_type(empty_dict, empty_dict, flags) == dict()
    assert flags['change'] is False


@pytest.fixture
def dict_of_none() -> Dict:
    return {'one': None}


def test_diff_type_dict_none(dict_of_none, empty_dict, flags):
    assert diff_type(None, dict_of_none, flags) == {'-one': None}
    assert flags['change'] is True
    flags['change'] = False

    assert diff_type(dict_of_none, None, flags) == {'+one': None}
    assert flags['change'] is True
    flags['change'] = False

    if flags['show_all']:
        assert diff_type(dict_of_none, dict_of_none, flags) == {'one': None}
    else:
        assert diff_type(dict_of_none, dict_of_none, flags) == dict()
        assert flags['change'] is False

    assert diff_type(empty_dict, dict_of_none, flags) == {"-one": None}
    assert flags['change'] is True

    flags['change'] = False
    assert diff_type(dict_of_none, empty_dict, flags) == {"+one": None}
    assert flags['change'] is True


@pytest.fixture
def dict_of_empty_str() -> Dict:
    return {'one': ''}


def test_diff_type_dict_of_empty_str(dict_of_empty_str, flags):
    if flags['show_all']:
        assert diff_type(dict_of_empty_str, dict_of_empty_str, flags) == {'one': ''}
    else:
        assert diff_type(dict_of_empty_str, dict_of_empty_str, flags) == dict()
    assert flags['change'] is False

    assert diff_type(None, dict_of_empty_str, flags) == {'-one': '-'}
    assert flags['change'] is True
    flags['change'] = False

    assert diff_type(dict_of_empty_str, None, flags) == {'+one': '+'}
    assert flags['change'] is True


@pytest.fixture
def dict_of_int() -> Dict:
    return {'one': 0}


def test_diff_type_dict_of_int(dict_of_int, flags):
    if flags['show_all']:
        assert diff_type(dict_of_int, dict_of_int, flags) == {'one': 0}
    else:
        assert diff_type(dict_of_int, dict_of_int, flags) == dict()
    assert flags['change'] is False

    assert diff_type(None, dict_of_int, flags) == {'-one': '-0'}
    assert flags['change'] is True

    flags['change'] = False
    assert diff_type(dict_of_int, None, flags) == {'+one': '+0'}
    assert flags['change'] is True


@pytest.fixture
def dict_of_empty_list() -> Dict:
    return {"one": []}


def test_diff_type_dict_of_empty_list(dict_of_empty_list, flags):
    if flags['show_all']:
        assert diff_type(dict_of_empty_list, dict_of_empty_list, flags) == {"one": []}
    else:
        assert diff_type(dict_of_empty_list, dict_of_empty_list, flags) == {}
    assert flags["change"] is False

    assert diff_type(None, dict_of_empty_list, flags) == {"-one": []}
    assert flags["change"] is True

    flags["change"] = False
    assert diff_type(dict_of_empty_list, None, flags) == {"+one": []}
    assert flags["change"] is True


@pytest.fixture
def dict_of_empty_dict() -> Dict:
    return {"one": {}}


def test_diff_type_dict_of_empty_dict(dict_of_empty_dict, flags):
    if flags['show_all']:
        assert diff_type(dict_of_empty_dict, dict_of_empty_dict, flags) == {"one": {}}
    else:
        assert diff_type(dict_of_empty_dict, dict_of_empty_dict, flags) == {}
    assert flags['change'] is False

    assert diff_type(None, dict_of_empty_dict, flags) == {"-one": {}}
    assert flags['change'] is True

    flags['change'] = False
    assert diff_type(dict_of_empty_dict, None, flags) == {"+one": {}}
    assert flags['change'] is True


@pytest.fixture
def empty_list() -> List:
    return list()


def test_diff_type_list(empty_list, flags):
    assert diff_type(empty_list, empty_list, flags) == list()
    assert flags["change"] is False
    assert diff_type(None, empty_list, flags) == list()
    assert flags["change"] is False
    assert diff_type(empty_list, None, flags) == list()
    assert flags["change"] is False


@pytest.fixture
def list_of_str() -> List:
    return ['']


def test_diff_type_list_of_str(list_of_str, flags):
    if flags['show_all']:
        assert diff_type(list_of_str, list_of_str, flags) == ['']
    else:
        assert diff_type(list_of_str, list_of_str, flags) == []

    assert diff_type(None, list_of_str, flags) == ['-']
    assert flags['change'] is True

    flags['change'] = False
    assert diff_type(list_of_str, None, flags) == ['+']
    assert flags['change'] is True


@pytest.fixture
def list_of_int() -> List:
    return [0]


def test_diff_type_list_of_int(list_of_int, flags):
    if flags['show_all']:
        assert diff_type(list_of_int, list_of_int, flags) == [0]
    else:
        assert diff_type(list_of_int, list_of_int, flags) == []
    assert flags['change'] is False

    assert diff_type(None, list_of_int, flags) == ['-0']
    assert flags['change'] is True

    flags['change'] = False
    assert diff_type(list_of_int, None, flags) == ['+0']
    assert flags['change'] is True


@pytest.fixture
def list_of_empty_dict() -> List[Dict]:
    return [{}]


def test_diff_type_list_of_dict(list_of_empty_dict, flags):
    with pytest.raises(NotImplementedError):
        assert diff_type(list_of_empty_dict, list_of_empty_dict, flags)

    with pytest.raises(NotImplementedError):
        assert diff_type(None, list_of_empty_dict, flags)

    with pytest.raises(NotImplementedError):
        assert diff_type(list_of_empty_dict, None, flags)


@pytest.fixture
def list_of_list() -> List[List]:
    return [[]]


def test_diff_type_list_of_list(list_of_list, flags):
    with pytest.raises(NotImplementedError):
        assert diff_type(list_of_list, list_of_list, flags)

    with pytest.raises(NotImplementedError):
        assert diff_type(None, list_of_list, flags)

    with pytest.raises(NotImplementedError):
        assert diff_type(list_of_list, None, flags)

