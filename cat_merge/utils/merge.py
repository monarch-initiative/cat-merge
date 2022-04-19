import pandas as pd
from pandas.core.frame import DataFrame
from typing import List
import os


def load_dataframes(files: List[str], add_provided_by: bool = True) -> List[DataFrame]:
    dataframes = []
    for file in files:
        df = pd.read_csv(file, sep="\t", dtype="string", lineterminator="\n")
        if add_provided_by:
            df["provided_by"] = os.path.basename(file)
        dataframes.append(df)
    return dataframes


def merge_dataframes(dataframes: List[DataFrame]) -> DataFrame:
    return pd.concat(dataframes, axis=0)


def get_duplicate_rows(df: DataFrame) -> DataFrame:
    return df[df.index.duplicated(keep=False)]


def clean_nodes(nodes: DataFrame) -> DataFrame:
    nodes.drop_duplicates(inplace=True)
    nodes.index.name = 'id'
    nodes.fillna("None", inplace=True)
    column_agg = {x: ' '.join for x in nodes.columns if x != 'id'}
    nodes.groupby(['id'], as_index=True).agg(column_agg)


def clean_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    return edges[edges.subject.isin(nodes.index) & edges.object.isin(nodes.index)]


def get_dangling_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    return edges[~edges.subject.isin(nodes.index) | ~edges.object.isin(nodes.index)]
