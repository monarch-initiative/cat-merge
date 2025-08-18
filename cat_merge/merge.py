import os
import tarfile
import time
from typing import List

import yaml
import logging

from cat_merge.file_utils import read_dfs, read_tar_dfs, get_files, write, write_qc
from cat_merge.merge_utils import merge_kg
from cat_merge.qc_utils import create_qc_report

log = logging.getLogger(__name__)


def merge(
        name: str = "merged-kg",
        source: str = None,  # Optional directory or tar archive containing node and edge files
        nodes: List[str] = None,  # Optional list of node files
        edges: List[str] = None,  # Optional list of edge files
        mappings: List[str] = None,  # Optional list of SSSOM mapping files
        output_dir: str = "merged-output",  # Directory to output knowledge graph
        qc_report: bool = True
):
    """
    Merges nodes and edges into a knowledge graph.

    Args:
        name (str): Output name of KG after merge.
        source (str, optional): Optional directory or tar archive containing node and edge files.
        nodes (List[str], optional): Optional list of node files.
        edges (List[str], optional): Optional list of edge files.
        mappings (List[str], optional): Optional list of SSSOM mapping files.
        output_dir (str): Directory to output knowledge graph.
        qc_report (bool, optional): Boolean for whether to generate a qc report (defaults to True).

    Returns:
        None
    """
    start_time = time.time()
    timing = {}
    
    print(f"""\
Merging KG files...
  name: {name} 
  source: {source} 
  nodes: {nodes}
  edges: {edges} 
  mappings: {mappings}
  output_dir: {output_dir}
""")
    if source is None and (nodes is None or edges is None):
        raise ValueError("Wrong attributes: must specify both nodes & edges or source")

    if source is not None and (nodes or edges):
        raise ValueError("Wrong attributes: source and node or edge files cannot both be specified")

    step_start = time.time()
    print("Reading node and edge files")
    if type(nodes) is list and len(nodes) > 0 \
            and type(edges) is list and len(edges) > 0:
        node_dfs = read_dfs(nodes)
        edge_dfs = read_dfs(edges)
    elif source is not None:
        if os.path.isdir(source):
            node_files, edge_files = get_files(source)
            node_dfs = read_dfs(node_files)
            edge_dfs = read_dfs(edge_files)
        elif tarfile.is_tarfile(source):
            tar = tarfile.open(source, "r:*")
            node_dfs = read_tar_dfs(tar, "_nodes")
            edge_dfs = read_tar_dfs(tar, "_edges")
        else:
            raise ValueError("Specified source is not a directory or tar archive")
    else:
        raise ValueError("Must specify either nodes & edges as lists or source as a directory or tar archive")

    mapping_dfs = []
    if mappings is not None:
        mapping_dfs = read_dfs(mappings, add_source_col=None, comment_character="#")
    timing['read_files'] = time.time() - step_start

    step_start = time.time()
    print("Merging...")
    kg, qc = merge_kg(node_dfs=node_dfs, edge_dfs=edge_dfs, mapping_dfs=mapping_dfs)
    
    write(
        name=name,
        kg=kg,
        output_dir=output_dir
    )

    write_qc(name=name, qc=qc, output_dir=output_dir)
    timing['merge_and_write'] = time.time() - step_start

    if qc_report:
        step_start = time.time()
        print("Generating QC report")
        qc_report = create_qc_report(kg, qc)

        with open(f"{output_dir}/qc_report.yaml", "w") as report_file:
            yaml.dump(qc_report, report_file)
        timing['qc_report'] = time.time() - step_start
    else:
        timing['qc_report'] = 0
    
    # Calculate total time
    total_time = time.time() - start_time
    timing['total'] = total_time
    
    print(f"\n=== TIMING REPORT (Pandas) ===")
    print(f"File reading:      {timing['read_files']:.2f}s")
    print(f"Merge & write:     {timing['merge_and_write']:.2f}s")
    print(f"QC report:         {timing['qc_report']:.2f}s")
    print(f"==============================")
    print(f"TOTAL TIME:        {total_time:.2f}s")
