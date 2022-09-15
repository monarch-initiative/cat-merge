import pandas as pd
# from grape import Graph  # type: ignore

from cat_merge.model.merged_kg import MergedKG
from typing import Dict, List, Union


def create_edge_report(edges_provided_by, edges_provided_by_values, unique_id_from_nodes) -> Dict:
    edge_object = {
        "name": edges_provided_by,
        "namespaces": col_to_yaml(get_namespace(
            pd.concat([edges_provided_by_values['subject'], edges_provided_by_values['object']])
        ).drop_duplicates()),
        # "namespaces": list(set((list(set(edges_provided_by_values['subject'].str.split(':').str[0]))) + (list(set(
        #     edges_provided_by_values['object'].str.split(':').str[0]))))),
        "categories": col_to_yaml(edges_provided_by_values['category']),
        "total_number": edges_provided_by_values['id'].size,
        # unique subjects and objects in edges but not in unique id nodes file
        "missing": len(get_missing(
            [edges_provided_by_values['subject'], edges_provided_by_values['object']], unique_id_from_nodes)),
        # "missing": list_difference(pd.concat(
        #     [edges_provided_by_values['subject'], edges_provided_by_values['object']]
        # ).drop_duplicates().sort_values().tolist(), unique_id_from_nodes.tolist()),
        # "missing": (len(set(edges_provided_by_values['subject']) - set(unique_id_from_nodes))) + (len(set(
        #     edges_provided_by_values['object']) - set(unique_id_from_nodes))),
        "predicates": [],
        "node_types": []
    }

    return edge_object


def create_predicate_report(edges_provided_by_values, unique_id_from_nodes) -> List[Dict]:
    predicates = []
    predicate_group = edges_provided_by_values.groupby(['predicate'])[['id', 'object', 'subject', 'category']]
    for predicate, predicate_values in predicate_group:
        predicate_object = {
            "uri": predicate,
            "total_number": predicate_values['id'].size,
            "missing_subjects": len(set(predicate_values['subject']) - set(unique_id_from_nodes)),
            "missing_objects": len(set(predicate_values['object']) - set(unique_id_from_nodes)),
            "missing_subject_namespaces":
                col_to_yaml(get_namespace(get_difference(predicate_values['subject'], unique_id_from_nodes))),
            # list(set([x.split(":")[0] for x in (set(predicate_values['subject']) - set(unique_id_from_nodes))])),
            "missing_object_namespaces":
                col_to_yaml(get_namespace(get_difference(predicate_values['object'], unique_id_from_nodes))),
            # list(set([x.split(":")[0] for x in (set(predicate_values['object']) - set(unique_id_from_nodes))]))
        }
        predicates.append(predicate_object)
    return predicates


def create_edge_node_types_report(edges_provided_by_values, nodes) -> List[Dict]:
    node_types = []
    # list of subjects and objects from edges file that are in nodes file
    subject_nodes = list(get_intersection(edges_provided_by_values['subject'], nodes["id"]))
    object_nodes = list(get_intersection(edges_provided_by_values['object'], nodes["id"]))
    node_type_list = subject_nodes + object_nodes
    # node_type_list = list(get_intersection(edges_provided_by_values['subject'], nodes["id"])) \
    #                  + list(get_intersection(edges_provided_by_values['object'], nodes["id"]))
    # node_type_list = (list(set(edges_provided_by_values['subject']) & set(nodes["id"]))) + (list(set(
    #     edges_provided_by_values['object']) & set(nodes["id"])))
    node_type_df = nodes[nodes['id'].isin(node_type_list)]
    node_grouping_fields = ['id', 'category']
    if 'in_taxon' in nodes.columns:
        node_grouping_fields.append('in_taxon')
    node_type_group = node_type_df.groupby(['provided_by'])[node_grouping_fields]
    for node_type_provided_by, node_type_provided_by_values in node_type_group:
        missing_subjects = get_difference(node_type_provided_by_values['id'], edges_provided_by_values['subject'])
        missing_objects = get_difference(node_type_provided_by_values['id'], edges_provided_by_values['object'])
        node_type_object = {
            "name": node_type_provided_by,
            "categories": col_to_yaml(node_type_provided_by_values['category']),
            "namespaces": col_to_yaml(get_namespace(node_type_provided_by_values['id'])),
            "total_number": node_type_provided_by_values['id'].size,
            # id that are in nodes file but are not in subject or object from edges file
            "missing": missing_subjects.size + missing_objects.size,
            # "missing": get_difference(node_type_provided_by_values['id'], edges_provided_by_values['subject']).size \
            #            + get_difference(node_type_provided_by_values['id'], edges_provided_by_values['object']).size,
            # "missing": len(set(node_type_provided_by_values['id']) - (set(edges_provided_by_values['subject'])))
            #            + len(set(node_type_provided_by_values['id']) - (set(edges_provided_by_values['object'])))
        }
        if 'in_taxon' in nodes.columns:
            node_type_object["taxon"] = col_to_yaml(node_type_provided_by_values["in_taxon"])
        node_types.append(node_type_object)
    return node_types


def create_edges_report(edges, nodes):
    edges_reports = []
    edges_group = edges.groupby(['provided_by'])[['id', 'object', 'subject', 'predicate', 'category']]

    for edges_provided_by, edges_provided_by_values in edges_group:
        edge_object = create_edge_report(edges_provided_by, edges_provided_by_values, nodes["id"])
        edge_object["predicates"] = create_predicate_report(edges_provided_by_values, nodes["id"])
        edge_object["node_types"] = create_edge_node_types_report(edges_provided_by_values, nodes)

        edges_reports.append(edge_object)

    return edges_reports


def get_namespace(col: pd.Series) -> pd.Series:
    return col if len(col) == 0 else col.str.split(':').str[0]


def col_to_yaml(col: pd.Series) -> List[str]:
    # This probably should have a better name
    # Convert a column from pandas to data for yaml report
    return col.drop_duplicates().sort_values().tolist()


def get_missing(cols: List[pd.Series], ids: pd.Series) -> List[str]:
    return get_difference(pd.concat(cols).drop_duplicates().sort_values().tolist(), ids.tolist())


def create_nodes_report(nodes: pd.DataFrame) -> List[Dict]:
    node_report = []
    node_grouping_fields = get_intersection(list(nodes.columns), ['id', 'category', 'in_taxon'])

    nodes_group = nodes.groupby(['provided_by'])[node_grouping_fields]
    for nodes_provided_by, nodes_provided_by_values in nodes_group:
        node_object = {
            "name": nodes_provided_by,
            "namespaces": col_to_yaml(get_namespace(nodes_provided_by_values['id'])),
            "categories": col_to_yaml(nodes_provided_by_values['category']),
            "total_number": nodes_provided_by_values['id'].size,
        }
        if 'in_taxon' in nodes.columns:
            node_object["taxon"] = col_to_yaml(nodes_provided_by_values["in_taxon"])
        node_report.append(node_object)
    return node_report


def cols_fill_na(df: pd.DataFrame, names_values: dict) -> pd.DataFrame:
    for col_name, fill_value in names_values.items():
        if col_name in df.columns:
            df[col_name] = df[col_name].fillna(fill_value)
    return df


def get_intersection(a: Union[List, pd.Series], b: Union[List, pd.Series]) -> Union[List, pd.Series]:
    if type(a) != type(b):
        raise ValueError("get_intersection: arguments must have the same type")
    elif not (type(a) is list or type(a) is pd.Series):
        raise ValueError("get_intersection: arguments must be of type list or pandas.Series")

    s = sorted(list(set(a) & set(b)))
    return s if type(a) is list else pd.Series(s, dtype=a.dtype, name=a.name)


def get_difference(a: Union[List, pd.Series], b: Union[List, pd.Series]) -> Union[List, pd.Series]:
    if type(a) != type(b):
        raise ValueError("get_difference: arguments must have the same type")
    elif not (type(a) is list or type(a) is pd.Series):
        raise ValueError("get_difference: arguments must be of type list or pandas.Series")

    s = sorted(list(set(a) - set(b)))
    return s if type(a) is list else pd.Series(s, dtype=a.dtype, name=a.name)


def create_qc_report(kg: MergedKG) -> Dict:
    """
    interface for generating qc report from merged kg
    :param kg: a MergeKG with data to create QC report
    :return: a dictionary representing the QC report
    """

    nodes = cols_fill_na(kg.nodes, {'in_taxon': 'missing taxon', 'category': 'missing category'})
    ingest_collection = {
        'nodes': create_nodes_report(nodes),
        # 'duplicate_nodes': create_nodes_report(kg.duplicate_nodes)
        'edges': create_edges_report(kg.edges, nodes),
        'dangling_edges': create_edges_report(kg.dangling_edges, nodes)
    }

    return ingest_collection
