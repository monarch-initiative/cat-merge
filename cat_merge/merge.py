import yaml
import logging

from cat_merge.file_utils import *
from cat_merge.merge_utils import *
from cat_merge.qc_utils import create_qc_report

log = logging.getLogger(__name__)

def merge(
        name: str = "merged-kg",
        input_dir: str = None,  # Optional directory containing node and edge files
        edges: List[str] = None,  # Optional list of edge files
        nodes: List[str] = None,  # Optional list of node files
        mappings: List[str] = None,  # Optional list of SSSOM mapping files
        output_dir: str = "merged-output",  # Directory to output knowledge graph
        merge_delimiter: str = "|",  # Delimiter to use when merging categories and properties on duplicates
        qc_report: bool = True
):

    print(f"""\
Merging KG files...
  name: {name} 
  input_dir: {input_dir} 
  nodes: {nodes}
  edges: {edges} 
  mappings: {mappings}
  output_dir: {output_dir}
""")

    print("Reading node and edge files")
    if nodes is not None and len(nodes) > 0 \
            and edges is not None and len(edges) > 0:
        node_dfs = read_dfs(nodes)
        edge_dfs = read_dfs(edges)
    elif input_dir is not None:
        node_files, edge_files = get_files(input_dir)
        node_dfs = read_dfs(node_files)
        edge_dfs = read_dfs(edge_files)

    mapping_dfs = []
    if mappings is not None:
        for file in mappings:
            mapping_dfs.append(read_df(file, add_provided_by=False))

    print("Merging...")
    kg = merge_kg(node_dfs=node_dfs, edge_dfs=edge_dfs, mapping_dfs=mapping_dfs, merge_delimiter=merge_delimiter)
    write(
        name=name,
        kg=kg,
        output_dir=output_dir
    )

    if qc_report:
        print("Generating QC report")
        qc_report = create_qc_report(kg)

        with open(f"{output_dir}/qc_report.yaml", "w") as report_file:
            yaml.dump(qc_report, report_file)


