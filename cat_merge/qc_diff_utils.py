import yaml
from typing import Dict, List, Union


def print_types(a: Union[Dict, List, str]):
    print(type(a))
    match a:
        case dict():
            for i, j in a.items():
                print(i)
                print_types(j)
        case list():
            for i in a:
                print_types(i)


# def create_qc_nodes(nodes: List) -> Dict:
#     qc_nodes = {}
#     for i in nodes:
#         qc_nodes[i.name] = i

# def create_qc_edges(edges: List) -> Dict:


def compare_nodes_qc(a_nodes: Union[List, None], b_nodes: Union[List, None]):
    # if a_nodes is None and b_nodes is None:
    #     return None
    node_compare = {}
    missing = ""

    a_names = get_source_names(a_nodes)
    b_names = get_source_names(b_nodes)
    all_names = list(a_names) + list(b_names)

    for name in all_names:
        if a_names.get(name) is None:
            a_source = get_empty(b_nodes[list(b_names).index(name)])
            missing = "-"
        else:
            a_source = a_nodes[list(a_names).index(name)]

        if b_names.get(name) is None:
            b_source = get_empty(a_nodes[list(a_names).index(name)])
            missing = "+"
        else:
            b_source = b_nodes[list(b_names).index(name)]

        source = {}
        for key in a_source.keys():
            source[missing + key] = diff_type(a_source.get(key), b_source.get(key))
        node_compare[name] = source
    return node_compare


# compare_nodes_qc(qc_yaml_a.get("nodes"), qc_yaml_b.get("nodes"))


def diff_type(a: Union[List, str, int], b: Union[List, str, int, None]):
    if type(a) != type(b) and not (a is None or b is None):
        msg = "diff_type: operands have different types. a: " + str(type(a)) + " b: " + str(type(b))
        raise TypeError(msg)

    diff: Union[Dict, List, int, str]
    case_type = a if a is not None else b
    match case_type:
        case list():
            if len(case_type) > 0 and type(case_type[0]) is dict:
                diff = compare_nodes_qc(a, b)
            else:
                diff = diff_lists(a, b)
        case str():
            diff = diff_str(a, b)
        case int():
            diff = diff_int(a, b)
        case None:
            diff = None
        case _:
            msg = "diff_type: type of operands not implemented: " + str(type(case_type))
            raise NotImplementedError(msg)

    return diff


def diff_lists(a: Union[List, None], b: Union[List, None]) -> List:
    diff = []
    a = a if a is not None else []
    b = b if b is not None else []
    for key in dict.fromkeys(a + b):
        if key in a and key in b:
            diff.append(key)
        elif key not in b:
            diff.append("+" + key)
        elif key not in a:
            diff.append("-" + key)
    return diff


def diff_str(a: str, b: str) -> Union[str, List]:
    diff: Union[str, List]
    if a == b:
        diff = a
    else:
        diff = ["+" + str(a), "-" + str(b)]
    return diff


def diff_int(a: int, b: int) -> Union[int, str, Dict]:
    diff: Union[int, Dict]
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


def get_empty(x: Union[List, Dict]) -> Union[List, Dict, None]:
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
            raise ValueError


def get_source_names(sources: Union[List[Dict], None]) -> Dict:
    if sources is None:
        return dict()

    names = dict()
    for s in sources:
        name = s.get("name")
        names[name] = s
    return names


def diff_yaml(a_yaml: Dict, b_yaml: Dict) -> Dict:
    yaml_qc_compare = {}
    for key in dict.fromkeys(list(a_yaml.keys()) + list(b_yaml.keys())):
        yaml_qc_compare[key] = compare_nodes_qc(a_yaml.get(key), b_yaml.get(key))

    return yaml_qc_compare
