import csv
import os
import tarfile
from pathlib import Path
import pandas as pd
# from grape import Graph  # type: ignore
from typing import IO, List, Optional, Tuple, Union


from pandas import DataFrame

from cat_merge.model.merged_kg import MergedKG


def get_files(filepath: str, nodes_match: str = "_nodes", edges_match: str = "_edges"):
    node_files = []
    edge_files = []
    for file in os.listdir(filepath):
        if nodes_match in file:
            node_files.append(f"{filepath}/{file}")
        elif edges_match in file:
            edge_files.append(f"{filepath}/{file}")
    return node_files, edge_files


def read_dfs(files: List[str], add_source_col: Optional[str] = "provided_by") -> List[pd.DataFrame]:
    dataframes = []
    for file in files:
        dataframes.append(read_df(file, add_source_col, Path(file).stem))
    return dataframes


def read_tar_dfs(tar: tarfile.TarFile, type_name, add_source_col: str = "provided_by") -> List[pd.DataFrame]:
    dataframes = []
    for member in tar.getmembers():
        if member.isfile() and type_name in member.name:
            dataframes.append(read_df(tar.extractfile(member), add_source_col, member.name))
    return dataframes


def read_df(fh: Union[str, IO[bytes]],
            add_source_col: Optional[str] = "provided_by",
            source_col_value: Optional[str] = None) -> pd.DataFrame:
    df = pd.read_csv(fh, sep="\t", dtype="string", lineterminator="\n", quoting=csv.QUOTE_NONE, comment='#')
    if add_source_col is not None:
        df[add_source_col] = source_col_value
    return df


# def read_tar(archive_path: str,
#              nodes_match: str = "_nodes",
#              edges_match: str = "_edges",
#              add_source_col: str = "provided_by"):
#     tar = tarfile.open(archive_path, "r:*")
#     node_dfs = read_tar_dfs(tar, nodes_match, add_source_col)
#     edge_dfs = read_tar_dfs(tar, edges_match, add_source_col)
#     return node_dfs, edge_dfs


def write_df(df: pd.DataFrame, filename: str):
    df.to_csv(filename, sep="\t", index=False)


def write_tar(tar_path: str, files: List[str], delete_files=True):
    tar = tarfile.open(tar_path, "w:gz")
    for file in files:
        tar.add(file, arcname=os.path.basename(file))
    tar.close()
    if delete_files:
        for file in files:
            os.remove(file)


def read_kg(source: str = None,
            node_match: str = "_node",
            edge_match: str = "_edge",
            duplicate_node_match: str = "duplicate-node",
            dangling_edge_match: str = "dangling-edge",
            node_file: str = None,
            edge_file: str = None,
            duplicate_node_file: str = None,
            dangling_edge_file: str = None,
            add_source_col: str = None) -> MergedKG:
    duplicate_nodes = []
    dangling_edges = []

    if source is not None:
        if node_file is not None or edge_file is not None \
                or duplicate_node_file is not None or dangling_edge_file is not None:
            raise ValueError("Wrong attributes: source and files cannot both be specified")
        elif os.path.isdir(source):
            [node_file], [edge_file] = get_files(source)
            nodes = read_df(node_file, add_source_col, node_file)
            edges = read_df(edge_file, add_source_col, edge_file)
            duplicate_files = get_files(source, duplicate_node_match, dangling_edge_match)
            if len(duplicate_files[0]) == 1:
                [duplicate_node_file] = duplicate_files[0]
                duplicate_nodes = read_df(duplicate_node_file, add_source_col, node_file)
            if len(duplicate_files[0]) == 1:
                [dangling_edge_file] = duplicate_files[1]
                dangling_edges = read_df(dangling_edge_file, add_source_col, node_file)
        elif tarfile.is_tarfile(source):
            tar = tarfile.open(source, "r:*")
            [nodes] = read_tar_dfs(tar, node_match, add_source_col)
            [edges] = read_tar_dfs(tar, edge_match, add_source_col)
            tar_duplicate_nodes = read_tar_dfs(tar, duplicate_node_match, add_source_col)
            if len(tar_duplicate_nodes) == 1:
                [duplicate_nodes] = tar_duplicate_nodes
            tar_dangling_edges = read_tar_dfs(tar, dangling_edge_match, add_source_col)
            if len(tar_dangling_edges) == 1:
                [dangling_edges] = tar_dangling_edges
        else:
            raise ValueError("source is not an archive or directory")
    elif node_file is not None and edge_file is not None:
        nodes = read_df(node_file, add_source_col, node_file)
        edges = read_df(edge_file, add_source_col, node_file)
        if duplicate_node_file is not None:
            duplicate_nodes = read_df(duplicate_node_file, add_source_col, node_file)
        if dangling_edge_file is not None:
            dangling_edges = read_df(dangling_edge_file, add_source_col, node_file)
    else:
        raise ValueError("Must specify either nodes & edges or source")

    kg = MergedKG(nodes, edges, duplicate_nodes, dangling_edges)
    return kg


def write(kg: MergedKG, name: str, output_dir: str):
    Path(f"{output_dir}/qc").mkdir(exist_ok=True, parents=True)

    duplicate_nodes_path = f"{output_dir}/qc/{name}-duplicate-nodes.tsv.gz"
    dangling_edges_path = f"{output_dir}/qc/{name}-dangling-edges.tsv.gz"
    nodes_path = f"{output_dir}/{name}_nodes.tsv"
    edges_path = f"{output_dir}/{name}_edges.tsv"
    tar_path = f"{output_dir}/{name}.tar.gz"

    write_df(df=kg.duplicate_nodes, filename=duplicate_nodes_path)
    write_df(df=kg.dangling_edges, filename=dangling_edges_path)
    write_df(df=kg.nodes, filename=nodes_path)
    write_df(df=kg.edges, filename=edges_path)

    write_tar(tar_path, [nodes_path, edges_path])
