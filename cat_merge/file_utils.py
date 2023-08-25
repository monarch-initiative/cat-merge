import csv
import os
import tarfile
from pathlib import Path
import pandas as pd
from typing import IO, List, Optional, Union

from cat_merge.model.merged_kg import MergedKG, MergeQC


def get_files(filepath: str, nodes_match: str = "_nodes", edges_match: str = "_edges"):
    """
    Get node and edge files in a directory based on a string match.

    Args:
        filepath (str): Path to directory.
        nodes_match (str): String to match for node files.
        edges_match (str): String to match for edge files.

    Returns:
        Tuple[List[str], List[str]]: List of node files and list of edge files.
    """
    node_files = []
    edge_files = []
    for file in os.listdir(filepath):
        if nodes_match in file:
            node_files.append(f"{filepath}/{file}")
        elif edges_match in file:
            edge_files.append(f"{filepath}/{file}")
    return node_files, edge_files


def read_dfs(files: List[str],
             add_source_col: Optional[str] = "provided_by",
             comment_character: str = None) -> List[pd.DataFrame]:
    """
    Read a list of files into dataframes.

    Args:
        files (List[str]): List of files.
        add_source_col (str, optional): Name of column to add to each dataframe with the name of the file.

    Returns:
        List[pandas.DataFrame]: List of dataframes.
    """
    dataframes = []
    for file in files:
        dataframes.append(read_df(file, add_source_col, Path(file).stem, comment_character=comment_character))
    return dataframes


def read_tar_dfs(tar: tarfile.TarFile, type_name, add_source_col: str = "provided_by") -> List[pd.DataFrame]:
    """
    Read a tar archive into dataframes.

    Args:
        tar (tarfile.TarFile): Tarfile object.
        type_name (str): String to match for node or edge files.
        add_source_col (str, optional): Name of column to add to each dataframe with the name of the file.

    Returns:
        List[pandas.DataFrame]: List of dataframes.
    """
    dataframes = []
    for member in tar.getmembers():
        if member.isfile() and type_name in member.name:
            dataframes.append(read_df(tar.extractfile(member), add_source_col, member.name))
    return dataframes


def read_df(fh: Union[str, IO[bytes]],
            add_source_col: Optional[str] = "provided_by",
            source_col_value: Optional[str] = None,
            comment_character: str = None) -> pd.DataFrame:
    """
    Read a file into a dataframe.

    Args:
        fh (str, io.TextIOWrapper): File handle.
        add_source_col (str, optional): Name of column to add to the dataframe with the name of the file.
        source_col_value (Any, optional): Value to add to the source column.
        comment_character (str, optional): Character to ignore lines starting with, or anything after.

    Returns:
        pandas.DataFrame: Dataframe.
    """
    df = pd.read_csv(fh, sep="\t", dtype="string", lineterminator="\n", quoting=csv.QUOTE_NONE, comment=comment_character)
    if add_source_col is not None:
        df[add_source_col] = source_col_value
    return df


def write_df(df: pd.DataFrame, filename: str):
    """
    Write a dataframe to a file.

    Args:
        df (pandas.DataFrame): Dataframe.
        filename (str): Name of file.

    Returns:
        None
    """
    df.to_csv(filename, sep="\t", index=False)


def write_tar(tar_path: str, files: List[str], delete_files=True):
    """
    Write a list of files to a tar archive.

    Args:
        tar_path (str): Path to tar archive.
        files (List[str]): List of files.
        delete_files (bool, optional): Delete files after writing to tar archive.

    Returns:
        None
    """
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
            node_file: str = None,
            edge_file: str = None,
            duplicate_node_file: str = None,
            dangling_edge_file: str = None,
            add_source_col: str = None) -> MergedKG:
    """
    Read a knowledge graph from a directory or tar archive.

    Args:
        source (str): Path to directory or tar archive.
        node_match (str): String to match for node files.
        edge_match (str): String to match for edge files.
        node_file (str, optional): Path to node file.
        edge_file (str, optional): Path to edge file.
        duplicate_node_file (str, optional): Path to duplicate node file.
        dangling_edge_file (str, optional): Path to dangling edge file.
        add_source_col (str, optional): Name of column to add to each dataframe with the name of the file.

    Returns:
        MergedKG: MergedKG object.
    """
    if source is not None:
        if node_file is not None or edge_file is not None \
                or duplicate_node_file is not None or dangling_edge_file is not None:
            raise ValueError("Wrong attributes: source and files cannot both be specified")
        elif os.path.isdir(source):
            [node_file], [edge_file] = get_files(source)
            nodes = read_df(node_file, add_source_col, node_file)
            edges = read_df(edge_file, add_source_col, edge_file)
        elif tarfile.is_tarfile(source):
            tar = tarfile.open(source, "r:*")
            [nodes] = read_tar_dfs(tar, node_match, add_source_col)
            [edges] = read_tar_dfs(tar, edge_match, add_source_col)
        else:
            raise ValueError("source is not an archive or directory")
    elif node_file is not None and edge_file is not None:
        nodes = read_df(node_file, add_source_col, node_file)
        edges = read_df(edge_file, add_source_col, node_file)
    else:
        raise ValueError("Must specify either nodes & edges or source")

    kg = MergedKG(nodes, edges)
    return kg


def read_qc(source: str = None) -> MergeQC:
    """
    Read a MergeQC object from a directory or tar archive.

    Args:
        source (str): Path to directory or tar archive.

    Returns:
        MergeQC: MergeQC object.
    """
    duplicate_nodes = pd.DataFrame([])
    dangling_edges = pd.DataFrame([])
    duplicate_edges = pd.DataFrame([])
    if source is not None:
        if os.path.isdir(source):
            qc_files = os.listdir(source)
            for qc_file in qc_files:
                if "duplicate-nodes" in qc_file:
                    duplicate_nodes = read_df(qc_file)
                elif "dangling-edges" in qc_file:
                    dangling_edges = read_df(qc_file)
                elif "duplicate-edges" in qc_file:
                    duplicate_edges = read_df(qc_file)
        elif tarfile.is_tarfile(source):
            tar = tarfile.open(source, "r:*")
            tar_duplicate_nodes = read_tar_dfs(tar, "duplicate-nodes")
            if len(tar_duplicate_nodes) == 1:
                [duplicate_nodes] = tar_duplicate_nodes
            tar_dangling_edges = read_tar_dfs(tar, "dangling-edges")
            if len(tar_dangling_edges) == 1:
                [dangling_edges] = tar_dangling_edges
            tar_duplicate_edges = read_tar_dfs(tar, "duplicate-edges")
            if len(tar_duplicate_edges) == 1:
                [duplicate_edges] = tar_duplicate_edges
        else:
            raise ValueError("source is not an archive or directory")
    else:
        raise ValueError("Must specify source")

    return MergeQC(duplicate_nodes=duplicate_nodes, dangling_edges=dangling_edges, duplicate_edges=duplicate_edges)


def write(kg: MergedKG, name: str, output_dir: str):
    """
    Write a knowledge graph to a directory.

    Args:
        kg (MergedKG): MergedKG object.
        name (str): Name of knowledge graph.
        output_dir (str): Path to directory.

    Returns:
        None
    """
    Path(f"{output_dir}/qc").mkdir(exist_ok=True, parents=True)

    nodes_path = f"{output_dir}/{name}_nodes.tsv"
    edges_path = f"{output_dir}/{name}_edges.tsv"
    tar_path = f"{output_dir}/{name}.tar.gz"

    write_df(df=kg.nodes, filename=nodes_path)
    write_df(df=kg.edges, filename=edges_path)

    write_tar(tar_path, [nodes_path, edges_path])

def write_qc(qc: MergeQC, name: str, output_dir: str):
    duplicate_nodes_path = f"{output_dir}/qc/{name}-duplicate-nodes.tsv.gz"
    dangling_edges_path = f"{output_dir}/qc/{name}-dangling-edges.tsv.gz"

    write_df(df=qc.duplicate_nodes, filename=duplicate_nodes_path)
    write_df(df=qc.dangling_edges, filename=dangling_edges_path)
