import pytest
from cat_merge.qc_diff_utils import diff_lists
from typing import List


@pytest.fixture
def list1() -> List:
    return ["item1", "item2", "item3", "item7"]


# def test_diff_lists_match(str1):
#     test_str_match = diff_str(str1, str1)
#     assert (test_str_match == "source1.tsv")
#
#
# def test_diff_str_a_none(str1):
#     test_str_a_none = diff_str(None, str1)
#     assert (test_str_a_none == "-source1.tsv")
#
#
# def test_diff_str_b_none(str1):
#     test_str_b_none = diff_str(str1, None)
#     assert (test_str_b_none == "+source1.tsv")
#
#
# @pytest.fixture
# def empty_list() -> List:
#     return []
#
#
# @pytest.fixture
# def str2() -> str:
#     return "source2.tsv"
#
#
# def test_diff_int_no_match(str1, str2):
#     test_str_no_match = diff_str(str1, str2)
#     result = ["+source1.tsv", "-source2.tsv"]
#     assert (test_str_no_match == result)
