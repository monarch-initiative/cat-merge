import inspect
from functools import wraps
from typing import Dict, List, Union


def validate_diff_args(f):
    @wraps(f)
    def wrapped_diff(*args, **kwargs):
        a = kwargs.get("a") if len(args) < 1 else args[0]
        b = kwargs.get("b") if len(args) < 2 else args[1]
        if type(a) != type(b) and not (a is None or b is None):
            message = f.__name__ + ": operands have different types. a: " + str(type(a)) + " b: " + str(type(b))
            raise TypeError(message)
        elif a is None and b is None:
            message = f.__name__ + ": both values to compare are None, this shouldn't happen."
            raise ValueError(message)
        return f(*args, **kwargs)
    return wrapped_diff


@validate_diff_args
def diff_yaml(a_yaml: Dict, b_yaml: Dict) -> Dict:
    yaml_qc_compare = {}
    for key in dict.fromkeys(list(a_yaml.keys()) + list(b_yaml.keys())):
        yaml_qc_compare[key] = diff_elem(a_yaml.get(key), b_yaml.get(key))

    return yaml_qc_compare


@validate_diff_args
def diff_elem(a: Union[Dict, List, None], b: Union[Dict, List, None]):
    # TODO detect if List[Dict] and only pull source names if True
    # TODO implement on Dict(not List[Dict]) elem
    node_compare = {}
    missing = ""

    a_dict = sources_dict(a)
    b_dict = sources_dict(b)

    all_keys = dict.fromkeys(list(a_dict.keys()) + list(b_dict.keys()))

    for outer_key in all_keys:
        if outer_key not in a_dict.keys():
            b_source = b_dict.get(outer_key)
            a_source = get_empty(b_dict.get(outer_key))
            missing = "-"
        elif outer_key not in b_dict.keys():
            a_source = a_dict.get(outer_key)
            b_source = get_empty(a_dict.get(outer_key))
            missing = "+"
        else:
            b_source = b_dict.get(outer_key)
            a_source = a_dict.get(outer_key)

        source = {}
        for inner_key in a_source.keys():
            source[missing + inner_key] = diff_type(a_source.get(inner_key), b_source.get(inner_key))
        node_compare[outer_key] = source
    return node_compare


@validate_diff_args
def diff_type(a: Union[List, str, int, None], b: Union[List, str, int, None]) -> Union[List, str, int, None]:

    diff: Union[Dict, List, int, str, None]
    case_type = a if a is not None else b
    match case_type:
        case dict():
            # TODO implement diff on dict -- Done
            diff = diff_dict(a, b)
        case list():
            if len(case_type) > 0 and type(case_type[0]) is dict:
                diff = diff_elem(a, b)
            else:
                diff = diff_list(a, b)
        case str():
            diff = diff_str(a, b)
        case int():
            diff = diff_int(a, b)
        case None:
            diff = None
        case _:
            message = "diff_type: type of operands not implemented: " + str(type(case_type))
            raise NotImplementedError(message)

    return diff


@validate_diff_args
def diff_dict(a: Union[Dict, None], b: Union[Dict, None]) -> Dict:
    diff = {}
    a = {} if a is None else a
    b = {} if b is None else b

    for key in dict.fromkeys(list(a.keys()), list(b.keys())):
        diff[key] = diff_type(a.get(key), b.get(key))

    return diff


@validate_diff_args
def diff_list(a: Union[List, None], b: Union[List, None]) -> List:
    diff = []
    a = [] if a is None else a
    b = [] if b is None else b
    a_as_keys = dict(zip(a, a))
    b_as_keys = dict(zip(b, b))

    for key in dict.fromkeys(a + b):
        diff.append(diff_type(a_as_keys.get(key), b_as_keys.get(key)))
    return diff


@validate_diff_args
def diff_str(a: Union[str, None], b: Union[str, None]) -> Union[str, List]:
    diff: Union[str, List]
    if a == b:
        diff = a
    elif a is None:
        diff = "-" + b
    elif b is None:
        diff = "+" + a
    else:
        diff = ["+" + a, "-" + b]
    return diff


@validate_diff_args
def diff_int(a: Union[int, None], b: Union[int, None]) -> Union[int, str, Dict]:
    diff: Union[int, str, Dict]
    if a == b:
        diff = a
    elif a is None:
        diff = "-" + str(b)
    elif b is None:
        diff = "+" + str(a)
    else:
        diff = {
            "change": a - b,
            "new": a,
            "old": b
        }
    return diff


def get_empty(x: Union[List, Dict, int, str]) -> Union[List, Dict, None]:
    """Create an empty version of the given object"""
    match x:
        case list():
            empty_list = []
            for i in x:
                empty = get_empty(i)
                if empty is not None:
                    empty_list.append(empty)
            return empty_list
        case dict():
            empty_dict = {}
            for key, value in x.items():
                if key == "name" or key == "uri":
                    empty_dict[key] = value
                elif value is None:
                    empty_dict[key] = None
                else:
                    empty_dict[key] = get_empty(value)
            return empty_dict
        case int() | str():
            return None
        case _:
            # We don't know how to deal with anything else, i.e. sets or tuples.
            message = "get_empty: Type not implemented: " + str(type(x))
            raise NotImplementedError(message)


def sources_dict(a: Union[Dict, List[Dict]]) -> Dict:
    a_dict = {}
    match a:
        case dict():
            a_dict = a
        case None:
            pass
        case list():
            for i in a:
                if i.get("name") is not None:
                    a_dict[i.get("name")] = i
                elif i.get("uri") is not None:
                    a_dict[i.get("uri")] = i
                else:
                    # We don't know how to handle a List[Dict] that doesn't have name or uri
                    message = "sources_dict: List[Dict] does not have a name key, aborting"
                    raise NotImplementedError(message)
        case _:
            # We shouldn't ever reach here, wrong type given.
            message = "source_dict: Wrong Type; type should be Dict, List or None."
            raise TypeError(message)

    return a_dict
