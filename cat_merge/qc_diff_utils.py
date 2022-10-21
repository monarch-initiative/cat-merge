from functools import wraps
from typing import Dict, List, Union


def validate_diff_args(f):
    @wraps(f)
    def wrapped_diff(*args, **kwargs):
        a = kwargs.get("a") if len(args) < 1 else args[0]
        b = kwargs.get("b") if len(args) < 2 else args[1]
        flags = kwargs.get("flags") if len(args) < 3 else args[2]

        if not isinstance(flags, dict) or \
                any(key not in flags for key in ("show_all", "change")) or \
                any(not isinstance(value, bool) for value in flags.values()):
            message = f.__name__ + ": bad flags -- flags dict must contain 'show_all' and 'match' as bool"
            raise KeyError(message)

        if type(a) != type(b) and not (a is None or b is None):
            message = f.__name__ + ": operands have different types. a: " + str(type(a)) + " b: " + str(type(b))
            raise TypeError(message)

        if a is None and b is None:
            message = f.__name__ + ": both values to compare are None, this shouldn't happen."
            raise ValueError(message)

        return f(*args, **kwargs)

    return wrapped_diff


# @validate_diff_args
def diff_yaml(a_yaml: Dict, b_yaml: Dict, show_all: bool) -> Dict:
    flags = {'show_all': show_all, 'change': False}

    yaml_qc_compare = {}
    for key in dict.fromkeys(list(a_yaml.keys()) + list(b_yaml.keys())):
        diff_value = diff_elem(a_yaml.get(key), b_yaml.get(key), flags)
        if flags['change'] or flags['show_all']:
            yaml_qc_compare[key] = diff_value
        flags['change'] = False

    return yaml_qc_compare


@validate_diff_args
def diff_elem(a: Union[Dict, List, None], b: Union[Dict, List, None], flags: Dict):
    node_compare = {}
    missing = ""

    a_dict = sources_dict(a)
    b_dict = sources_dict(b)

    all_keys = dict.fromkeys(list(a_dict.keys()) + list(b_dict.keys()))

    change = False
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
            diff_value = diff_type(a_source.get(inner_key), b_source.get(inner_key), flags)
            if flags["change"] or flags["show_all"]:
                source[missing + inner_key] = diff_value
            change = any([change, flags["change"]])
            flags["change"] = False

        node_compare[missing + outer_key] = source

    flags['change'] = change
    return node_compare


@validate_diff_args
def diff_type(
        a: Union[List, Dict, str, int, None],
        b: Union[List, Dict, str, int, None],
        flags: Dict
) -> Union[List, str, int, None]:
    diff: Union[Dict, List, int, str, None]
    case_type = a if a is not None else b
    match case_type:
        case dict():
            diff = diff_dict(a, b, flags)
        case list():
            if len(case_type) > 0 and type(case_type[0]) is dict:
                diff = diff_elem(a, b, flags)
            else:
                diff = diff_list(a, b, flags)
        case str():
            diff = diff_str(a, b, flags)
        case int():
            diff = diff_int(a, b, flags)
        case None:
            diff = None
        case _:
            message = "diff_type: type of operands not implemented: " + str(type(case_type))
            raise NotImplementedError(message)

    return diff


@validate_diff_args
def diff_dict(a: Union[Dict, None], b: Union[Dict, None], flags: Dict) -> Dict:
    if a is None or b is None:
        change = True
    else:
        change = False

    diff = {}
    a = {} if a is None else a
    b = {} if b is None else b

    missing = ""
    for key in dict.fromkeys(list(a.keys()) + list(b.keys())):
        if key not in a.keys():
            missing = "-"
        elif key not in b.keys():
            missing = "+"

        if a.get(key) is None and b.get(key) is None:
            diff_value = None
            if not (key in a.keys() and key in b.keys()):
                flags['change'] = True
        else:
            diff_value = diff_type(a.get(key), b.get(key), flags)

        if flags["change"] or flags["show_all"] or missing != "":
            diff[missing + key] = diff_value
        change = any([change, flags["change"]])
        flags["change"] = False

    flags["change"] = change
    return diff


@validate_diff_args
def diff_list(a: Union[List, None], b: Union[List, None], flags: Dict) -> List:
    diff = []
    a = [] if a is None else a
    b = [] if b is None else b

    # Check if either list contains a list
    if any(isinstance(n, list) and len(n) == 0 for n in a + b):
        message = "diff_list: found list containing list, structure not supported."
        raise NotImplementedError(message)

    a_as_keys = dict(zip(a, a))
    b_as_keys = dict(zip(b, b))

    change = False
    for key in dict.fromkeys(a + b):
        diff_value = diff_type(a_as_keys.get(key), b_as_keys.get(key), flags)
        if flags["change"] or flags["show_all"]:
            diff.append(diff_value)
        change = any([change, flags["change"]])
        flags["change"] = False

    flags["change"] = change
    return diff


@validate_diff_args
def diff_str(a: Union[str, None], b: Union[str, None], flags: Dict) -> Union[str, List, None]:
    flags["change"] = True
    diff: Union[str, List, None]
    if a == b:
        flags["change"] = False
        if flags["show_all"]:
            diff = a
        else:
            diff = None
    elif a is None:
        diff = "-" + b
    elif b is None:
        diff = "+" + a
    else:
        diff = ["+" + a, "-" + b]
    return diff


@validate_diff_args
def diff_int(a: Union[int, None], b: Union[int, None], flags: Dict) -> Union[int, str, Dict, None]:
    flags["change"] = True
    diff: Union[int, str, Dict, None]
    if a == b:
        flags["change"] = False
        if flags["show_all"]:
            diff = a
        else:
            diff = None
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
            if not all(isinstance(x, dict) for x in a):
                # all list entries must be dict
                message = "sources_dict: List contains non-dict entries, aborting"
                raise ValueError(message)

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
