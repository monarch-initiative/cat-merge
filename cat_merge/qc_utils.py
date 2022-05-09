import pandas as pd
import os
from pprint import pprint
import yaml
from cat_merge.model.merged_kg import MergedKG
from typing import Dict

# def create_qc_report(mergedkg: MergedKG) -> Dict:
#     """
#     interface for generating qc report from merged kg
#     :param mergedkg:
#     :return:
#     """
# def get_edge_df_file_list():
# def get_edge_df_by_provided():
# def create_edge_object():

all_files_list = os.listdir("../data/transform_output/")

edge_dfs = []
node_dfs = []

for edge_file in all_files_list:
    if "edges" in edge_file and "ontology" not in edge_file:
        temp_edge_df = pd.read_csv(f"../data/transform_output/{edge_file}", sep="\t", dtype="string", lineterminator="\n")
        temp_edge_df["provided_by"] = edge_file
        edge_dfs.append(temp_edge_df)

for node_file in all_files_list:
    if "nodes" in node_file:
        temp_node_df = pd.read_csv(f"../data/transform_output/{node_file}", sep="\t", dtype="string", lineterminator="\n")
        temp_node_df["provided_by"] = node_file
        node_dfs.append(temp_node_df)

edges = pd.concat(edge_dfs, axis=0)
nodes = pd.concat(node_dfs, axis=0)
nodes['in_taxon'] = nodes['in_taxon'].fillna('missing taxon')
nodes['category'] = nodes['category'].fillna('missing category')
unique_id_from_nodes = nodes["id"]

ingest_collection = {
    "edges": [],
    "nodes": []
}

# Edges

edges_group = edges.groupby(['provided_by'])[['id', 'object', 'subject', 'predicate', 'category']]

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

    # list of subjects and objects from edges file that are in nodes file
    node_type_list = (list(set(edges_provided_by_values['subject']) & set(unique_id_from_nodes))) + (list(set(
            edges_provided_by_values['object']) & set(unique_id_from_nodes)))
    node_type_df = nodes[nodes['id'].isin(node_type_list)]
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

    ingest_collection['edges'].append(edge_object)


# Nodes

nodes_group = nodes.groupby(['provided_by'])[['id', 'category', 'in_taxon']]
for nodes_provided_by, nodes_provided_by_values in nodes_group:
    node_object = {
        "name": nodes_provided_by,
        "namespaces": list(set(list(set(nodes_provided_by_values['id'].str.split(':').str[0])))),
        "categories": list(set(nodes_provided_by_values['category'])),
        "total_number": len(set(nodes_provided_by_values['id'].tolist())),
        "taxon": list(set(nodes_provided_by_values["in_taxon"]))
    }
    ingest_collection['nodes'].append(node_object)

# Yaml output file for QC Report

with open("../output/qc_report.yaml", "w") as qc_report:
    yaml.dump(ingest_collection, qc_report)

pprint(ingest_collection)
