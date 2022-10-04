import pytest
from cat_merge.qc_diff_utils import validate_diff_args
from tests.test_utils import flags_params


@pytest.mark.parametrize("flags", flags_params.values(), ids=list(flags_params.keys()))
def test_validate_diff_args(list1, flags):
    @validate_diff_args
    def test_decorator(a, b, flags):
        pass

    with pytest.raises(KeyError):
        test_decorator("", "", flags="")

    with pytest.raises(KeyError):
        test_decorator("", "", flags={})

    with pytest.raises(KeyError):
        test_decorator("", "", flags={'change': False})

    with pytest.raises(KeyError):
        test_decorator("", "", flags={'show_all': False})

    with pytest.raises(KeyError):
        test_decorator("", "", flags={'show_all': ""})

    with pytest.raises(ValueError):
        test_decorator(None, None, flags)

    with pytest.raises(TypeError):
        test_decorator({}, [], flags)
