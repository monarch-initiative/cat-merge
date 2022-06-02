import pytest
from tests.test_utils import string_df
from cat_merge.merge_utils import get_duplicate_rows
from pandas.core.frame import DataFrame


@pytest.fixture
def dataframe_with_duplicates() -> DataFrame:
    A = u"""\
    id      category 
    Gene:1  Gene 
    Gene:2  Gene 
    Gene:2  Gene
    Gene:3  Gene
    """

    return string_df(A)


def test_get_duplicate_row(dataframe_with_duplicates):
    df = get_duplicate_rows(dataframe_with_duplicates)
    assert(len(df) == 2)
    assert(list(df.id) == ["Gene:2", "Gene:2"])

