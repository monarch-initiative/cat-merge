import copy
import pandas as pd
from io import StringIO
from itertools import zip_longest

import pytest
from pandas.core.frame import DataFrame


# Borrowed from https://stackoverflow.com/questions/58771331/cleanly-hard-code-a-pandas-dataframe-into-a-python-script
def string_df(data: str, index_column_is_id=True):
    if index_column_is_id:
        df = pd.read_csv(StringIO(data), sep=r"\s+", engine='python')
    else:
        df = pd.read_csv(StringIO(data), sep=r"\s+", engine='python')
    return df


def value(df: DataFrame, id: str, column: str):
    return list(df.loc[df['id'] == id][column])[0]


def flags_params():
    params = {
        # "change_T_show_T": {"change": True, "show_all": True},
        "change_F_show_T": {"change": False, "show_all": True},
        # "change_T_show_F": {"change": True, "show_all": False},
        "change_F_show_F": {"change": False, "show_all": False},
    }
    return copy.deepcopy(params)


def pytest_generate_tests(metafunc):
    if "flags" in metafunc.fixturenames:
        metafunc.parametrize("flags", flags_params().values(), ids=list(flags_params().keys()))


def check_report_values(report_values, expected_values):
    for val, expect in zip_longest(report_values, expected_values):
        assert val == expect
