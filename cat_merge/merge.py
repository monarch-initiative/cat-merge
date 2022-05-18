import typer
import yaml
import logging

from cat_merge.file_utils import *
from cat_merge.merge_utils import *
from cat_merge.qc_utils import create_qc_report

log = logging.getLogger(__name__)


def merge(
    name: str = typer.Option("merged-kg", help="Name of the resulting knowledge graph"),
    input_dir: str = typer.Option(None, help="Optional directory containing node and edge files"),
    edges: List[str] = typer.Option(None, help="Optional list of edge files"),
    nodes: List[str] = typer.Option(None, help="Optional list of node files"),
    mapping: str = typer.Option(None, help="Optional SSSOM mapping file"),
    output_dir: str = typer.Option("merged-output", help="Directory to output knowledge graph"),
    merge_delimiter: str = typer.Option("|", help="Delimiter to use when merging categories and properties on duplicates")
    ):

    print(f"""\
Merging KG files...
  name: {name} 
  input_dir: {input_dir} 
  nodes: {nodes}
  edges: {edges} 
  output_dir: {output_dir}
""")

    print("Reading node and edge files")
    if isinstance(nodes, List) and len(nodes) > 0 and isinstance(edges, List) and len(edges) > 0:
        node_dfs = read_dfs(nodes)
        edge_dfs = read_dfs(edges)
    elif input_dir is not None:
        node_files, edge_files = get_files(input_dir)
        node_dfs = read_dfs(node_files)
        edge_dfs = read_dfs(edge_files)

    mapping_df = None
    if mapping is not None:
        mapping_df = read_df()

    print("Merging...")
    kg = merge_kg(node_dfs=node_dfs, edge_dfs=edge_dfs, mapping=mapping_df, merge_delimiter=merge_delimiter)
    write(
        name=name,
        kg=kg,
        output_dir=output_dir
    )

    print("Generating QC report")
    qc_report = create_qc_report(kg)

    with open(f"{output_dir}/qc_report.yaml", "w") as report_file:
        yaml.dump(qc_report, report_file)


if __name__ == "__main__":
    typer.run(merge)
