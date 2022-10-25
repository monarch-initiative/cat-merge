import pytest
from cat_merge.qc_utils import ReportContainer


def test_report_container_no_args():
    rc = ReportContainer()
    assert rc.data_type == list
    assert rc.key_name == 'name'
    assert type(rc.data) == list
    assert rc.data == []


def test_report_container_key_only():
    rc = ReportContainer(key_name='other')
    assert rc.data_type == list
    assert rc.key_name == 'other'
    assert type(rc.data) == list
    assert rc.data == []


@pytest.mark.parametrize("data_type,expected", [
    pytest.param(list, [], id="list"),
    pytest.param(dict, {}, id="dict"),
])
def test_report_container_data_type(data_type, expected):
    rc = ReportContainer(data_type)
    assert rc.data_type == data_type
    assert rc.key_name == 'name'
    assert type(rc.data) == data_type
    assert rc.data == expected


@pytest.mark.parametrize("data_type,key_name,expected", [
    pytest.param(list, 'other', [], id="list-other"),
    pytest.param(dict, 'other', {}, id="dict-other"),
])
def test_report_container_(data_type, key_name, expected):
    rc = ReportContainer(data_type, key_name)
    assert rc.data_type == data_type
    assert rc.key_name == key_name
    assert type(rc.data) == data_type
    assert rc.data == expected


@pytest.mark.parametrize("data_type,key_name,addend,expected", [
    pytest.param(list, 'name', {'name': "one"}, [{'name': "one"}], id="list"),
    pytest.param(dict, 'name', {'name': "one"}, {'name': {'name': "one"}}, id="dict"),
])
def test_report_container_add(data_type, key_name, addend, expected):
    rc = ReportContainer(data_type, key_name)
    rc.add(addend)
    assert rc.data == expected


def test_report_container_invalid_type():
    with pytest.raises(ValueError):
        rc = ReportContainer(type)


def test_report_container_missing_key():
    rc = ReportContainer(dict)
    with pytest.raises(KeyError) as key_error:
        rc.add({})
    assert str(key_error.value) == '"ReportContainer: key: \'name\' missing from dict to add."'


def test_report_container_add_same_key():
    rc = ReportContainer(dict)
    rc.add({'name': "name"})
    with pytest.raises(KeyError) as key_error:
        rc.add({'name': "name"})
    assert str(key_error.value) == '"ReportContainer: key: \'name\' already added to data."'


def test_report_container_bad_data():
    rc = ReportContainer(dict)
    rc.data = set()
    with pytest.raises(RuntimeError) as runtime_error:
        rc.add({'name': "name"})
    assert str(runtime_error.value) == \
           'ReportContainer: attempting to add to invalid data type: ' + str(set)
