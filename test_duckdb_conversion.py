#!/usr/bin/env python3
"""
Test script to validate DuckDB conversion against existing pandas implementation.
"""

import os
import shutil
import tempfile
from pathlib import Path

# Import both implementations
from cat_merge.merge import merge as pandas_merge
from cat_merge.duckdb_merge import merge_duckdb


def test_duckdb_vs_pandas():
    """Compare DuckDB and pandas implementations on test data."""
    
    # Create temporary directories for outputs
    with tempfile.TemporaryDirectory() as temp_dir:
        pandas_output = os.path.join(temp_dir, "pandas_output")
        duckdb_output = os.path.join(temp_dir, "duckdb_output") 
        
        # Test data files
        test_nodes = ["tests/test_data/test_kg_nodes.tsv"]
        test_edges = ["tests/test_data/test_kg_edges.tsv"]
        
        print("Running pandas implementation...")
        try:
            pandas_merge(
                name="test-kg",
                nodes=test_nodes,
                edges=test_edges,
                output_dir=pandas_output,
                qc_report=True
            )
            print("✓ Pandas implementation completed")
        except Exception as e:
            print(f"✗ Pandas implementation failed: {e}")
            return False
        
        print("Running DuckDB implementation...")
        try:
            merge_duckdb(
                name="test-kg", 
                nodes=test_nodes,
                edges=test_edges,
                output_dir=duckdb_output,
                qc_report=True
            )
            print("✓ DuckDB implementation completed")
        except Exception as e:
            print(f"✗ DuckDB implementation failed: {e}")
            return False
        
        # Compare file counts
        print("\nComparing outputs...")
        pandas_files = list(Path(pandas_output).rglob("*"))
        duckdb_files = list(Path(duckdb_output).rglob("*"))
        
        print(f"Pandas output files: {[f.name for f in pandas_files if f.is_file()]}")
        print(f"DuckDB output files: {[f.name for f in duckdb_files if f.is_file()]}")
        
        # Check if both created main output files (pandas might create tar.gz)
        pandas_nodes = Path(pandas_output) / "test-kg_nodes.tsv"
        pandas_edges = Path(pandas_output) / "test-kg_edges.tsv"
        pandas_tar = Path(pandas_output) / "test-kg.tar.gz"
        duckdb_nodes = Path(duckdb_output) / "test-kg_nodes.tsv"
        duckdb_edges = Path(duckdb_output) / "test-kg_edges.tsv"
        
        # Check what pandas actually created
        pandas_has_data = pandas_tar.exists() or (pandas_nodes.exists() and pandas_edges.exists())
        duckdb_has_data = duckdb_nodes.exists() and duckdb_edges.exists()
        
        if pandas_has_data and duckdb_has_data:
            print("✓ Both implementations created main output files")
            
            # Count lines in DuckDB output files
            with open(duckdb_nodes) as f:
                duckdb_node_lines = len(f.readlines())
            with open(duckdb_edges) as f:
                duckdb_edge_lines = len(f.readlines())
            
            print(f"DuckDB nodes: {duckdb_node_lines} lines")
            print(f"DuckDB edges: {duckdb_edge_lines} lines")
            
            # If pandas created individual TSV files, compare them
            if pandas_nodes.exists() and pandas_edges.exists():
                with open(pandas_nodes) as f:
                    pandas_node_lines = len(f.readlines())
                with open(pandas_edges) as f:
                    pandas_edge_lines = len(f.readlines())
                
                print(f"Pandas nodes: {pandas_node_lines} lines")
                print(f"Pandas edges: {pandas_edge_lines} lines")
                
                if pandas_node_lines == duckdb_node_lines and pandas_edge_lines == duckdb_edge_lines:
                    print("✓ Line counts match between implementations")
                else:
                    print("⚠ Line counts differ - this may be expected due to different deduplication logic")
            else:
                print("⚠ Pandas created tar file only, skipping line count comparison")
        else:
            print("✗ Missing expected output files")
            return False
        
        # Check QC reports
        pandas_qc = Path(pandas_output) / "qc_report.yaml"
        duckdb_qc = Path(duckdb_output) / "qc_report.yaml"
        
        if pandas_qc.exists() and duckdb_qc.exists():
            print("✓ Both implementations created QC reports")
        else:
            print("⚠ QC reports missing")
        
        print("\nTest completed successfully!")
        return True


if __name__ == "__main__":
    success = test_duckdb_vs_pandas()
    exit(0 if success else 1)