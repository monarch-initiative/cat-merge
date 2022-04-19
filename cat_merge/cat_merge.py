import typer
from typing import List
import os
import tarfile
from .utils.merge import (load_dataframes,
                          merge_dataframes,
                          get_duplicate_rows,
                          clean_nodes,
                          clean_edges,
                          get_dangling_edges)
from .utils.file import write_df
from .model.merged_kg import MergedKG


def merge_kg(edge_files: List[str], node_files: List[str]) -> MergedKG:

    node_dfs = load_dataframes(node_files)
    edge_dfs = load_dataframes(edge_files)

    all_nodes = merge_dataframes(node_dfs)
    all_edges = merge_dataframes(edge_dfs)

    duplicate_nodes = get_duplicate_rows(all_nodes)
    dangling_edges = get_dangling_edges(all_edges)

    nodes = clean_nodes(all_nodes)
    edges = clean_edges(all_edges)

    return MergedKG(nodes=nodes, edges=edges, duplicate_nodes=duplicate_nodes, dangling_edges=dangling_edges)



def merge_wrapper(name: str, output_dir: str, node_files: List[str], edge_files: List[str]):

    merged_kg = merge_kg(node_files=node_files, edge_files=edge_files)




if __name__ == "__main__":
    typer.run(merge)
