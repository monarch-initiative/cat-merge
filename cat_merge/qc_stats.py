from typing import List, Dict

import yaml
from grape import Graph  # type: ignore


def load_graph(name: str, version: str, edges_path: str,
               nodes_path: str) -> Graph:
    """
    Load a graph with Ensmallen (from grape).

    Params:
        name (str): OBO Name of graph
        version (str): OBO Version of graph
        edges_path (str): Path to edge file
        nodes_path (str): Path to node file

    Returns:
        ensmallen Graph object
    """

    loaded_graph = Graph.from_csv(name=f"{name}_version_{version}",
                                  edge_path=edges_path,
                                  sources_column="subject",
                                  destinations_column="object",
                                  edge_list_header=True,
                                  edge_list_separator="\t",
                                  node_path=nodes_path,
                                  nodes_column="id",
                                  node_list_header=True,
                                  node_list_separator="\t",
                                  directed=False,
                                  verbose=True
                                  )

    return loaded_graph


def create_stats_report(g: Graph) -> List[Dict]:
    """
    Create a stats report for a graph.

    Params:
        g (Graph): Graph to create report for

    Returns:
        List of Dicts containing stats
    """
    node_count = g.get_number_of_nodes()
    edge_count = g.get_number_of_edges()
    connected_components = g.get_number_of_connected_components()
    singleton_count = g.get_number_of_singleton_nodes()
    max_node_degree = g.get_maximum_node_degree()
    mean_node_degree = g.get_node_degrees_mean()

    graph_stats = {"Nodes": node_count,
                   "Edges": edge_count,
                   "ConnectedComponents": connected_components,
                   "Singletons": singleton_count,
                   "MaxNodeDegree": max_node_degree,
                   "MeanNodeDegree": "{:.2f}".format(mean_node_degree)}

    return [graph_stats]


def qc_stats_report(
        nodes_file_name: str = None,
        edges_file_name: str = None,
        output_dir: str = None,
        output_name: str = "qc_stats_report.yaml"):
    """
    Generates a qc report for the knowledge graph

    Params:
        nodes_file_name (str): Path to node file
        edges_file_name (str): Path to edge file
        output_dir (str): Directory to output qc report
        output_name (str): Name of qc report file (defaults to "qc_report.yaml")

    Returns:
        None
    """
    g = load_graph(name="test_name",
                   version="0.1",
                   edges_path=edges_file_name,
                   nodes_path=nodes_file_name)
    report = create_stats_report(g)

    with open(f"{output_dir}/{output_name}", "w") as report_file:
        yaml.dump(report, report_file)
