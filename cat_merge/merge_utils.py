import pandas as pd
from pandas.core.frame import DataFrame
from typing import List, Tuple
from cat_merge.model.merged_kg import MergedKG, MergeQC
from cat_merge.mapping_utils import apply_mappings
import numpy as np


def concat_dataframes(dataframes: List[DataFrame]) -> DataFrame:
    """
    Concatenate a list of dataframes

    Args:
        dataframes: A list of pandas DataFrames to be concatenated

    Returns:
        A concatenated pandas DataFrame
    """
    return pd.concat(dataframes, axis=0)


def get_duplicates_by_id(df: DataFrame) -> DataFrame:
    """
    Get duplicate rows in a DataFrame based on the id column.

    Args:
        df (pandas.DataFrame): DataFrame of nodes.

    Returns:
        pandas.DataFrame: DataFrame of duplicate rows in the input DataFrame.
    """
    return df[df.id.duplicated(keep=False)]


def clean_nodes(nodes: DataFrame) -> DataFrame:
    """
    Clean nodes by dropping duplicates

    Args:
        nodes (pandas.DataFrame): Dataframe of nodes
    Returns:
        pandas.DataFrame: Dataframe of nodes with duplicates dropped
    """
    nodes.drop_duplicates(inplace=True)
    return nodes


def clean_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    """
    Clean edges by dropping duplicate and dangling edges

    Args:
        edges: A pandas DataFrame of edges.
        nodes: A pandas DataFrame of nodes.

    Returns:
        A pandas DataFrame of edges with no duplicate or dangling edges.
    """
    edges = edges.drop_duplicates(subset=['id'], keep=False)
    return edges[edges.subject.isin(nodes.id) & edges.object.isin(nodes.id)]


def get_dangling_edges(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    """
    Get dangling edges.

    Args:
        edges (pandas.DataFrame): DataFrame of edges.
        nodes (pandas.DataFrame): DataFrame of nodes.

    Returns:
        pandas.DataFrame: DataFrame of dangling edges.
    """
    dangling_edges = edges[~edges.subject.isin(nodes.id) | ~edges.object.isin(nodes.id)]
    return dangling_edges


def merge_kg(edge_dfs: List[DataFrame],
             node_dfs: List[DataFrame],
             mapping_dfs: List[DataFrame] = None) -> tuple[MergedKG, MergeQC]:
    """
    Merge a list of node and edge dataframes.

    Args:
        edge_dfs (List[pandas.DataFrame]): List of edge dataframes.
        node_dfs (List[pandas.DataFrame]): List of node dataframes.
        mapping_dfs (List[pandas.DataFrame]): List of mapping dataframes.

    Returns:
        Tuple[MergedKG, pandas.DataFrame]: A tuple containing the merged KG and merge QC.
    """
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

    nodes = clean_nodes(nodes=all_nodes)
    edges = clean_edges(edges=all_edges, nodes=nodes)

    return MergedKG(nodes=nodes, edges=edges), \
        MergeQC(duplicate_nodes=duplicate_nodes, duplicate_edges=duplicate_edges, dangling_edges=dangling_edges)
