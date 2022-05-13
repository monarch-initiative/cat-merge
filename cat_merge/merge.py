from typing import List
from cat_merge.file_utils import *
from cat_merge.merge_utils import *

def merge(
    name: str = "merged-kg",#typer.Option("merged-kg", help="Name of the resulting knowledge graph"),
    input_dir: str = None,#typer.Option(None, help="Optional directory containing node and edge files"),
    edges: List[str] = None,#typer.Option(None, help="Optional list of edge files"),
    nodes: List[str] = None,#typer.Option(None, help="Optional list of node files"),
    mapping: str = None,#typer.Option(None, help="Optional SSSOM mapping file")
    output_dir: str = "merged-output",#typer.Option("merged-output", help="Directory to output knowledge graph")
    merge_delimiter: str = "|",#typer.Option("|", help="Delimiter to use when merging categories and properties on duplicates")
    ):

    print(f"Merging KG files...\nName: {name} // input_dir: {input_dir} // nodes: {nodes} // edges: {edges} // output_dir: {output_dir}")

    if nodes is not None and edges is not None:
        node_dfs = read_dfs(nodes)
        edge_dfs = read_dfs(edges)
    elif input_dir is not None:
        node_files, edge_files = get_files(input_dir)
        node_dfs = read_dfs(node_files)
        edge_dfs = read_dfs(edge_files)

    mapping_df = None
    if mapping is not None:
        mapping_df = read_df()

    write(
        name=name,
        kg=merge_kg(node_dfs=node_dfs, edge_dfs=edge_dfs, mapping=mapping_df, merge_delimiter=merge_delimiter),
        output_dir=output_dir
    )
