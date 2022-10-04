import pytest
import copy

flags_params = {"change_T_show_T": {"change": True, "show_all": True},
                "change_F_show_T": {"change": False, "show_all": True},
                "change_T_show_F": {"change": True, "show_all": False},
                "change_F_show_F": {"change": False, "show_all": False}}


def pytest_generate_tests(metafunc):
    if "flags" in metafunc.fixturenames:
        metafunc.parametrize("flags", flags_params.values(), ids=list(flags_params.keys()))
