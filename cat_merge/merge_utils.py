import pandas as pd
from pandas.core.frame import DataFrame
from typing import List, Tuple
from cat_merge.model.merged_kg import MergedKG, MergeQC
from cat_merge.mapping_utils import apply_mappings
import numpy as np


def concat_dataframes(dataframes: List[DataFrame]) -> DataFrame:
    return pd.concat(dataframes, axis=0)


def get_duplicates_by_id(df: DataFrame) -> DataFrame:
    return df[df.id.duplicated(keep=False)]


def clean_nodes(nodes: DataFrame, merge_delimiter: str = " ") -> DataFrame:
    nodes.drop_duplicates(inplace=True)
    return nodes


def clean_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    edges = edges.drop_duplicates(subset=['id'], keep=False)
    return edges[edges.subject.isin(nodes.id) & edges.object.isin(nodes.id)]


def get_dangling_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    dangling_edges = edges[~edges.subject.isin(nodes.id) | ~edges.object.isin(nodes.id)]
    return dangling_edges


def merge_kg(edge_dfs: List[DataFrame],
             node_dfs: List[DataFrame],
             mapping_dfs: List[DataFrame] = None,
             merge_delimiter: str = "|") -> tuple[MergedKG, MergeQC]:
    all_nodes = concat_dataframes(node_dfs)
    all_nodes = all_nodes.fillna(np.nan).replace([np.nan], [None])
    all_edges = concat_dataframes(edge_dfs)
    all_edges = all_edges.fillna(np.nan).replace([np.nan], [None])

    if mapping_dfs is not None and len(mapping_dfs) > 0:
        mapping_df = concat_dataframes(mapping_dfs)
        all_edges = apply_mappings(all_edges, mapping_df)

    duplicate_nodes = get_duplicates_by_id(df=all_nodes)
    duplicate_edges = get_duplicates_by_id(df=all_edges)
    dangling_edges = get_dangling_edges(edges=all_edges, nodes=all_nodes)

    nodes = clean_nodes(nodes=all_nodes, merge_delimiter=merge_delimiter)
    edges = clean_edges(edges=all_edges, nodes=nodes)

    return MergedKG(nodes=nodes, edges=edges), \
        MergeQC(duplicate_nodes=duplicate_nodes, duplicate_edges=duplicate_edges, dangling_edges=dangling_edges)
