import os
import time
import yaml
from typing import List

from cat_merge.duckdb_utils import (
    read_kg_files, 
    read_mapping_files,
    apply_mappings,
    merge_and_clean,
    create_qc_aggregations,
    write_output_files
)
from cat_merge.duckdb_qc import create_qc_report_duckdb


def merge_duckdb(
    name: str = "merged-kg",
    source: str = None,
    nodes: List[str] = None,
    edges: List[str] = None,
    mappings: List[str] = None,
    output_dir: str = "merged-output",
    qc_report: bool = True
):
    """
    Merge knowledge graph files using DuckDB for improved performance.
    
    Args:
        name: Output name of KG after merge
        source: Optional directory containing node and edge files
        nodes: Optional list of node files
        edges: Optional list of edge files  
        mappings: Optional list of SSSOM mapping files
        output_dir: Directory to output knowledge graph
        qc_report: Whether to generate a QC report (defaults to True)
    """
    start_time = time.time()
    timing = {}
    
    print(f"""\
Merging KG files with DuckDB...
  name: {name} 
  source: {source} 
  nodes: {nodes}
  edges: {edges} 
  mappings: {mappings}
  output_dir: {output_dir}
""")
    
    # Validate arguments
    if source is None and (nodes is None or edges is None):
        raise ValueError("Wrong attributes: must specify both nodes & edges or source")

    if source is not None and (nodes or edges):
        raise ValueError("Wrong attributes: source and node or edge files cannot both be specified")
    
    # Read files into DuckDB
    step_start = time.time()
    print("Reading node and edge files into DuckDB...")
    conn = read_kg_files(source=source, nodes=nodes, edges=edges)
    timing['read_files'] = time.time() - step_start
    
    # Read mappings if provided
    if mappings:
        step_start = time.time()
        print("Reading mapping files...")
        read_mapping_files(conn, mappings)
        print("Applying mappings...")
        apply_mappings(conn)
        timing['mappings'] = time.time() - step_start
    else:
        timing['mappings'] = 0
    
    # Perform merge and cleaning operations
    step_start = time.time()
    print("Merging and cleaning data...")
    merge_and_clean(conn)
    timing['merge_clean'] = time.time() - step_start
    
    # Create aggregation tables for QC reporting
    step_start = time.time()
    print("Creating QC aggregations...")
    create_qc_aggregations(conn)
    timing['qc_aggregations'] = time.time() - step_start
    
    # Write output files
    step_start = time.time()
    print("Writing output files...")
    write_output_files(conn, name, output_dir)
    timing['write_files'] = time.time() - step_start
    
    # Generate QC report if requested
    if qc_report:
        step_start = time.time()
        print("Generating QC report...")
        qc_report_data = create_qc_report_duckdb(conn)
        
        with open(f"{output_dir}/qc_report.yaml", "w") as report_file:
            yaml.dump(qc_report_data, report_file, default_flow_style=False)
        timing['qc_report'] = time.time() - step_start
    else:
        timing['qc_report'] = 0
    
    # Close connection
    conn.close()
    
    # Calculate total time
    total_time = time.time() - start_time
    timing['total'] = total_time
    
    print(f"\n=== TIMING REPORT ===")
    print(f"File reading:      {timing['read_files']:.2f}s")
    print(f"Mappings:          {timing['mappings']:.2f}s")
    print(f"Merge & clean:     {timing['merge_clean']:.2f}s")
    print(f"QC aggregations:   {timing['qc_aggregations']:.2f}s")
    print(f"Write files:       {timing['write_files']:.2f}s")
    print(f"QC report:         {timing['qc_report']:.2f}s")
    print(f"=====================")
    print(f"TOTAL TIME:        {total_time:.2f}s")
    print(f"\nMerge completed! Output written to {output_dir}")


if __name__ == "__main__":
    # Example usage
    merge_duckdb(
        name="test-kg",
        source="tests/test_data",
        output_dir="test-output"
    )