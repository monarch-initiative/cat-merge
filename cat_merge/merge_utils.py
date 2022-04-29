import pandas as pd
from pandas.core.frame import DataFrame
from typing import List
import os
from cat_merge.model.merged_kg import MergedKG


def concat_dataframes(dataframes: List[DataFrame]) -> DataFrame:
    return pd.concat(dataframes, axis=0)


def get_duplicate_rows(df: DataFrame) -> DataFrame:
    return df[df.index.duplicated(keep=False)]


def clean_nodes(nodes: DataFrame) -> DataFrame:
    nodes.reset_index(inplace=True)
    nodes.drop_duplicates(inplace=True)
    nodes = nodes.rename(columns={'index': 'id'})
    nodes.fillna("None", inplace=True)
    column_agg = {x: ' '.join for x in nodes.columns if x != 'id'}
    nodes = nodes.groupby(['id'], as_index=True).agg(column_agg)
    return nodes


def clean_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    return edges[edges.subject.isin(nodes.index) & edges.object.isin(nodes.index)]


def get_dangling_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    return edges[~edges.subject.isin(nodes.index) | ~edges.object.isin(nodes.index)]


def merge_kg(edge_dfs: List[DataFrame], node_dfs: List[DataFrame]) -> MergedKG:

    all_nodes = concat_dataframes(node_dfs)
    all_edges = concat_dataframes(edge_dfs)

    duplicate_nodes = get_duplicate_rows(df=all_nodes)
    dangling_edges = get_dangling_edges(edges=all_edges, nodes=all_nodes)

    nodes = clean_nodes(nodes=all_nodes)
    edges = clean_edges(edges=all_edges, nodes=nodes)

    return MergedKG(nodes=nodes, edges=edges, duplicate_nodes=duplicate_nodes, dangling_edges=dangling_edges)
