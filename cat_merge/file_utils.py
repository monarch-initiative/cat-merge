from pandas.core.frame import DataFrame
import os
import tarfile
from typing import List
from model.merged_kg import MergedKG
import pandas as pd


def read_dfs(files: List[str], add_provided_by: bool = True) -> List[DataFrame]:
    dataframes = []
    for file in files:
        df = pd.read_csv(file, sep="\t", dtype="string", lineterminator="\n", index_col='id')
        df.index.name = 'id'
        if add_provided_by:
            df["provided_by"] = os.path.basename(file)
        dataframes.append(df)
    return dataframes


def write_df(df: DataFrame, filename: str):
    df.to_csv(filename, sep="\t")


def write_tar(tar_path: str, files: List[str], delete_files=True):
    tar = tarfile.open(tar_path, "w:gz")
    for file in files:
        tar.add(file)
    tar.close()
    if delete_files:
        for file in files:
            os.remove(file)


def write(kg: MergedKG, name: str, output_dir: str):

    os.makedirs(output_dir, exist_ok=True)

    duplicate_nodes_path = f"{output_dir}/{name}-duplicate-nodes.tsv.gz"
    dangling_edges_path = f"{output_dir}/{name}-dangling-edges.tsv.gz"
    nodes_path = f"{output_dir}/{name}_nodes.tsv"
    edges_path = f"{output_dir}/{name}_edges.tsv"
    tar_path = f"{output_dir}/{name}.tar.gz"

    write_df(df=kg.duplicate_nodes, filename=duplicate_nodes_path)
    write_df(df=kg.dangling_edges, filename=dangling_edges_path)
    write_df(df=kg.nodes, filename=nodes_path)
    write_df(df=kg.edges, filename=edges_path)

    write_tar(tar_path, [nodes_path, edges_path])



