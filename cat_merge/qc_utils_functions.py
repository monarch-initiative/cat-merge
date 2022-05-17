import pandas as pd
import os
from pprint import pprint
import yaml
from cat_merge.model.merged_kg import MergedKG
from typing import Dict

directory = os.listdir("../data/transform_output/")

# def create_qc_report(mergedkg: MergedKG) -> Dict:
#     """
#     interface for generating qc report from merged kg
#     :param mergedkg:
#     :return:
#     """


# get all edges file list
def get_edge_file_list():
    edge_file_list = []
    for edge_file in directory:
        if "edges" in edge_file and "ontology" not in edge_file:
            temp_edge_file_list = edge_file
            edge_file_list.append(temp_edge_file_list)
    return edge_file_list


# get node file list
def get_node_file_list():
    node_file_list = []
    for node_file in directory:
        if "nodes" in node_file:
            temp_node_file_list = node_file
            node_file_list.append(temp_node_file_list)
    return node_file_list


# get edge dataframes
def get_edge_dfs():
    edge_dfs = []
    edge_file_list = get_edge_file_list()
    for edge_file in edge_file_list:
        temp_edge_df = pd.read_csv(f"../data/transform_output/{edge_file}", sep="\t", dtype="string", lineterminator="\n")
        temp_edge_df["provided_by"] = edge_file
        edge_dfs.append(temp_edge_df)
    return edge_dfs


# get edge dataframes
def get_node_dfs():
    node_dfs = []
    node_file_list = get_node_file_list()
    for node_file in node_file_list:
        temp_node_df = pd.read_csv(f"../data/transform_output/{node_file}", sep="\t", dtype="string", lineterminator="\n")
        temp_node_df["provided_by"] = node_file
        node_dfs.append(temp_node_df)
    return node_dfs


# concat edge dfs
def concat_edge_dfs():
    edge_dfs = get_edge_dfs()
    edges = pd.concat(edge_dfs, axis=0)
    return edges


# concat node dfs
def concat_node_dfs():
    node_dfs = get_node_dfs()
    nodes = pd.concat(node_dfs, axis=0)
    nodes['in_taxon'] = nodes['in_taxon'].fillna('missing taxon')
    nodes['category'] = nodes['category'].fillna('missing category')
    return nodes


unique_id_from_nodes = concat_node_dfs()["id"]


# create edge object
def create_edge_object():
    edge_object_list = {"edges": []}
    edge_dfs = concat_edge_dfs()
    edges_group = edge_dfs.groupby(['provided_by'])[['id', 'object', 'subject', 'predicate', 'category']]
    for edges_provided_by, edges_provided_by_values in edges_group:
        edge_object = {
            "name": edges_provided_by,
            "namespaces": list(set((list(set(edges_provided_by_values['subject'].str.split(':').str[0]))) + (list(set(
                edges_provided_by_values['object'].str.split(':').str[0]))))),
            "categories": list(set(edges_provided_by_values['category'])),
            "total_number": len(edges_provided_by_values['id'].tolist()),
            # unique subjects and objects in edges but not in unique id nodes file
            "missing": (len(set(edges_provided_by_values['subject']) - set(unique_id_from_nodes))) + (len(set(
                edges_provided_by_values['object']) - set(unique_id_from_nodes))),
            "predicates": [],
            "node_types": []
        }
        predicate_group = edges_provided_by_values.groupby(['predicate'])[['id', 'object', 'subject', 'category']]
        for predicate, predicate_values in predicate_group:
            predicate_object = {
                "uri": predicate,
                "total_number": len(predicate_values['id'].tolist()),
                "missing_subjects": len(set(predicate_values['subject']) - set(unique_id_from_nodes)),
                "missing_objects": len(set(predicate_values['object']) - set(unique_id_from_nodes)),
                "missing_subject_namespaces": list(
                    set([x.split(":")[0] for x in (set(predicate_values['subject']) - set(unique_id_from_nodes))])),
                "missing_object_namespaces": list(
                    set([x.split(":")[0] for x in (set(predicate_values['object']) - set(unique_id_from_nodes))]))
            }
            edge_object['predicates'].append(predicate_object)
        node_type_list = (list(set(edges_provided_by_values['subject']) & set(unique_id_from_nodes))) + (list(set(
            edges_provided_by_values['object']) & set(unique_id_from_nodes)))
        node_type_df = concat_node_dfs()[concat_node_dfs()['id'].isin(node_type_list)]
        node_type_group = node_type_df.groupby(['provided_by'])[['id', 'category', 'in_taxon']]
        for node_type_provided_by, node_type_provided_by_values in node_type_group:
            node_type_object = {
                "name": node_type_provided_by,
                "categories": list(set(node_type_provided_by_values['category'])),
                "taxon": list(set(node_type_provided_by_values["in_taxon"])),
                "namespaces": list(set(list(set(node_type_provided_by_values['id'].str.split(':').str[0])))),
                "total_number": len(set(node_type_provided_by_values['id'].tolist())),
                # id that are in nodes file but are not in subject or object from edges file
                "missing": len(set(node_type_provided_by_values['id']) - (set(edges_provided_by_values['subject'])))
                           + len(set(node_type_provided_by_values['id']) - (set(edges_provided_by_values['object'])))
            }
            edge_object['node_types'].append(node_type_object)
        edge_object_list['edges'].append(edge_object)
    return edge_object_list['edges']


# create node object
def create_node_object():
    node_object_list = {"nodes": []}
    node_dfs = concat_node_dfs()
    nodes_group = node_dfs.groupby(['provided_by'])[['id', 'category', 'in_taxon']]
    for nodes_provided_by, nodes_provided_by_values in nodes_group:
        node_object = {
            "name": nodes_provided_by,
            "namespaces": list(set(list(set(nodes_provided_by_values['id'].str.split(':').str[0])))),
            "categories": list(set(nodes_provided_by_values['category'])),
            "total_number": len(set(nodes_provided_by_values['id'].tolist())),
            "taxon": list(set(nodes_provided_by_values["in_taxon"]))
        }
        node_object_list['nodes'].append(node_object)
    return node_object_list['nodes']


# create ingest collection object
def create_ingest_collection_object():
    edge_object = create_edge_object()
    node_object = create_node_object()
    ingest_collection = {
        "edges": edge_object,
        "nodes": node_object
    }
    return ingest_collection


# create yaml qc report
def create_yaml_qc_report():
    ingest_collection_object = create_ingest_collection_object()
    with open("../output/qc_report.yaml", "w") as qc_report:
        yaml.dump(ingest_collection_object, qc_report)
    return qc_report


create_yaml_qc_report()



