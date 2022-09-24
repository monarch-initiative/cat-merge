from functools import wraps
from typing import Any, Dict, List, Union


def diff_args(f):
    @wraps(f)
    def check_diff_args(*args, **kwargs):
        a = kwargs.get("a") if len(args) < 1 else args[0]
        b = kwargs.get("b") if len(args) < 2 else args[1]
        if type(a) != type(b) and not either_none(a, b):
            message = f.__name__ + ": operands have different types. a: " + str(type(a)) + " b: " + str(type(b))
            raise TypeError(message)
        elif both_none(a, b):
            message = f.__name__ + ": both values to compare are None, this shouldn't happen."
            raise ValueError(message)
        return f(*args, **kwargs)
    return check_diff_args


def both_none(a: Any, b: Any):
    return a is None and b is None


def either_none(a: Any, b: Any):
    return a is None or b is None


def diff_yaml(a_yaml: Dict, b_yaml: Dict) -> Dict:
    yaml_qc_compare = {}
    for key in dict.fromkeys(list(a_yaml.keys()) + list(b_yaml.keys())):
        yaml_qc_compare[key] = diff_elem(a_yaml.get(key), b_yaml.get(key))

    return yaml_qc_compare


def diff_elem(a_nodes: Union[List, None], b_nodes: Union[List, None]):
    node_compare = {}
    missing = ""

    a_names = get_source_names(a_nodes)
    b_names = get_source_names(b_nodes)
    all_names = dict.fromkeys(a_names + b_names)

    for name in all_names:
        if name not in a_names:
            b_source = b_nodes[b_names.index(name)]
            a_source = get_empty(b_source)
            missing = "-"
        elif name not in b_names:
            a_source = a_nodes[a_names.index(name)]
            b_source = get_empty(a_source)
            missing = "+"
        else:
            b_source = b_nodes[b_names.index(name)]
            a_source = a_nodes[a_names.index(name)]

        source = {}
        for key in a_source.keys():
            source[missing + key] = diff_type(a_source.get(key), b_source.get(key))
        node_compare[name] = source
    return node_compare


def diff_type(a: Union[List, str, int, None], b: Union[List, str, int, None]) -> Union[List, str, int, None]:
    if type(a) != type(b) and a is not None and b is not None:
        message = "diff_type: operands have different types. a: " + str(type(a)) + " b: " + str(type(b))
        raise TypeError(message)
    elif a is None and b is None:
        message = "diff_type: both values to compare are None, this shouldn't happen."
        raise ValueError(message)

    diff: Union[Dict, List, int, str, None]
    case_type = a if a is not None else b
    match case_type:
        case list():
            if len(case_type) > 0 and type(case_type[0]) is dict:
                diff = diff_elem(a, b)
            else:
                diff = diff_lists(a, b)
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


@diff_args
def diff_lists(a: Union[List, None], b: Union[List, None]) -> List:
    if type(a) is list and type(b) is list and len(a) > 0 and len(b) > 0 and isinstance(a[0], type(b[0])):
        message = "diff_lists: list elements have different types. a: " + str(type(a[0])) + " b: " + str(type(b[0]))
        raise TypeError(message)
    diff = []
    a = [] if a is None else a
    b = [] if b is None else b
    for key in dict.fromkeys(a + b):
        if key in a and key in b:
            diff.append(key)
        elif key not in b:
            diff.append("+" + key)
        elif key not in a:
            diff.append("-" + key)
    return diff


def diff_str(a: Union[str, None], b: Union[str, None]) -> Union[str, List]:
    diff: Union[str, List]
    if a is None and b is None:
        message = "diff_str: both values to compare are None, this shouldn't happen."
        raise ValueError(message)
    elif a == b:
        diff = a
    elif a is None:
        diff = "-" + b
    elif b is None:
        diff = "+" + a
    else:
        diff = ["+" + a, "-" + b]
    return diff


def diff_int(a: Union[int, None], b: Union[int, None]) -> Union[int, str, Dict]:
    diff: Union[int, str, Dict]
    if a is None and b is None:
        message = "diff_int: both values to compare are None, this shouldn't happen."
        raise ValueError(message)
    elif a == b:
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
                if value is None:
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


def get_source_names(sources: Union[List[Dict], None]) -> List:
    if sources is None:
        return list()

    names = list()
    for s in sources:
        names.append(s.get("name"))
    return names
