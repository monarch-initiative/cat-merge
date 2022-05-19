from cat_merge.model.merged_kg import MergedKG
from typing import Dict, List


def create_edge_report(edges_provided_by, edges_provided_by_values, unique_id_from_nodes) -> Dict:
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

    return edge_object


def create_predicate_report(edges_provided_by_values, unique_id_from_nodes) -> List[Dict]:
    predicates = []
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
        predicates.append(predicate_object)
    return predicates


def create_edge_node_types_report(edges_provided_by_values, unique_id_from_nodes, nodes):
    node_types = []
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
        node_types.append(node_type_object)
    return node_types


def create_edges_report(edges, unique_id_from_nodes, nodes):

    edges_reports = []
    edges_group = edges.groupby(['provided_by'])[['id', 'object', 'subject', 'predicate', 'category']]

    for edges_provided_by, edges_provided_by_values in edges_group:
        edge_object = create_edge_report(edges_provided_by, edges_provided_by_values, unique_id_from_nodes)
        edge_object["predicates"] = create_predicate_report(edges_provided_by_values, unique_id_from_nodes)
        edge_object["node_types"] = create_edge_node_types_report(edges_provided_by_values, unique_id_from_nodes, nodes)

        edges_reports.append(edge_object)

    return edges_reports


def create_qc_report(merged_kg: MergedKG) -> Dict:
    """
    interface for generating qc report from merged kg
    :param mergedkg:
    :return: a dictionary representing the QC report
    """

    edges = merged_kg.edges
    dangling_edges = merged_kg.dangling_edges
    nodes = merged_kg.nodes

    nodes['in_taxon'] = nodes['in_taxon'].fillna('missing taxon')
    nodes['category'] = nodes['category'].fillna('missing category')

    # convert the index back into an id column for nodes
    nodes.reset_index(inplace=True)
    nodes = nodes.rename(columns={'index': 'id'})

    unique_id_from_nodes = nodes["id"]

    # convert the index back into an id column for edges
    edges.reset_index(inplace=True)
    edges = edges.rename(columns={'index': 'id'})

    # convert the index back into an id column for dangling_edges
    dangling_edges.reset_index(inplace=True)
    dangling_edges = dangling_edges.rename(columns={'index': 'id'})


    ingest_collection = {
        "edges": [],
        "nodes": [],
        "dangling_edges": []
    }

    # Edges
    ingest_collection['edges'] = create_edges_report(edges, unique_id_from_nodes, nodes)
    ingest_collection['dangling_edges'] = create_edges_report(dangling_edges, unique_id_from_nodes, nodes)


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

    return ingest_collection

