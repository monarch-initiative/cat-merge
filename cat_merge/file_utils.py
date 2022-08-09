import csv
import os, tarfile
from pathlib import Path
import pandas as pd
from typing import List, Optional

from cat_merge.model.merged_kg import MergedKG


def get_files(filepath: str):
    node_files = []
    edge_files = []
    for file in os.listdir(filepath):
        if file.endswith('nodes.tsv'):
            node_files.append(f"{filepath}/{file}")
        elif file.endswith('edges.tsv'):
            edge_files.append(f"{filepath}/{file}")
    return node_files, edge_files


def read_dfs(files: List[str], add_provided_by: bool = True) -> List[pd.DataFrame]:
    dataframes = []
    for file in files:
        dataframes.append(read_df(file, add_provided_by=add_provided_by))
    return dataframes


def read_df(file: str, add_provided_by: bool = True):
    df = pd.read_csv(file, sep="\t", dtype="string", lineterminator="\n", quoting=csv.QUOTE_NONE, comment='#')

    if add_provided_by:
        df["provided_by"] = os.path.basename(file)
    return df


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

def read_kg(archive_path: str,
            dangling_edges_path: Optional[str],
            nodes_file_name: Optional[str],
            edges_file_name: Optional[str]) -> MergedKG:
    if not os.path.exists(archive_path):
        raise FileNotFoundError
    if dangling_edges is not None and not os.path.exists(dangling_edges_path)
        raise FileNotFoundError

    # iterate over files in tar, pull _nodes and _edges

    # read into pandas and return a MergedKG instance



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
