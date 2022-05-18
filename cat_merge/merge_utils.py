import pandas as pd
from pandas.core.frame import DataFrame
from typing import List
from cat_merge.model.merged_kg import MergedKG
from cat_merge.mapping_utils import apply_mappings

def concat_dataframes(dataframes: List[DataFrame]) -> DataFrame:
    return pd.concat(dataframes, axis=0)


def get_duplicate_rows(df: DataFrame) -> DataFrame:
    return df[df.index.duplicated(keep=False)]


def clean_nodes(nodes: DataFrame, merge_delimiter: str = " ") -> DataFrame:
    nodes.reset_index(inplace=True)
    nodes.drop_duplicates(inplace=True)
    nodes = nodes.rename(columns={'index': 'id'})
    column_agg = {x: merge_delimiter.join for x in nodes.columns if x != 'id'}
    nodes = nodes.groupby(['id'], as_index=True).agg(column_agg)
    return nodes


def clean_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    return edges[edges.subject.isin(nodes.index) & edges.object.isin(nodes.index)]


def get_dangling_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    dangling_edges = edges[~edges.subject.isin(nodes.index) | ~edges.object.isin(nodes.index)]
    return dangling_edges


def merge_kg(edge_dfs: List[DataFrame], node_dfs: List[DataFrame], mapping: DataFrame = None, merge_delimiter: str = "|") -> MergedKG:
    all_nodes = concat_dataframes(node_dfs)
    all_nodes.fillna("None", inplace=True)
    all_edges = concat_dataframes(edge_dfs)
    all_edges.fillna("None", inplace=True)

    if mapping is not None:
        all_edges = apply_mappings(all_edges, mapping)

    duplicate_nodes = get_duplicate_rows(df=all_nodes)
    dangling_edges = get_dangling_edges(edges=all_edges, nodes=all_nodes)

    nodes = clean_nodes(nodes=all_nodes, merge_delimiter=merge_delimiter)
    edges = clean_edges(edges=all_edges, nodes=nodes)

    return MergedKG(nodes=nodes, edges=edges, duplicate_nodes=duplicate_nodes, dangling_edges=dangling_edges)
