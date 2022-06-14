import pandas as pd
from pandas.core.frame import DataFrame
from typing import List
from cat_merge.model.merged_kg import MergedKG
from cat_merge.mapping_utils import apply_mappings


def concat_dataframes(dataframes: List[DataFrame]) -> DataFrame:
    return pd.concat(dataframes, axis=0)


def get_duplicate_rows(df: DataFrame) -> DataFrame:
    return df[df.id.duplicated(keep=False)]


def clean_nodes(nodes: DataFrame, merge_delimiter: str = " ") -> DataFrame:
    nodes.drop_duplicates(inplace=True)
    return nodes


def clean_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    return edges[edges.subject.isin(nodes.id) & edges.object.isin(nodes.id)]


def get_dangling_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    dangling_edges = edges[~edges.subject.isin(nodes.id) | ~edges.object.isin(nodes.id)]
    return dangling_edges


def merge_kg(edge_dfs: List[DataFrame], node_dfs: List[DataFrame], mapping_dfs: List[DataFrame] = None, merge_delimiter: str = "|") -> MergedKG:
    all_nodes = concat_dataframes(node_dfs)
    all_nodes.fillna("None", inplace=True)
    all_edges = concat_dataframes(edge_dfs)
    all_edges.fillna("None", inplace=True)

    if mapping_dfs is not None and len(mapping_dfs) > 0:
        mapping_df = concat_dataframes(mapping_dfs)
        all_edges = apply_mappings(all_edges, mapping_df)

    duplicate_nodes = get_duplicate_rows(df=all_nodes)
    dangling_edges = get_dangling_edges(edges=all_edges, nodes=all_nodes)

    nodes = clean_nodes(nodes=all_nodes, merge_delimiter=merge_delimiter)
    edges = clean_edges(edges=all_edges, nodes=nodes)

    return MergedKG(nodes=nodes, edges=edges, duplicate_nodes=duplicate_nodes, dangling_edges=dangling_edges)
