#!/usr/bin/env python3
"""
Test that DuckDB now matches pandas behavior for provided_by generation.
"""

import tempfile
import os
from pathlib import Path
import duckdb


def test_duckdb_individual_files():
    """Test DuckDB individual file processing matches pandas."""
    print("=== Testing Individual File Processing ===")
    
    test_files = [
        "my_source_nodes.tsv",
        "another_data_source_nodes.tsv", 
        "simple_edges.tsv",
        "complex_name_with_underscores_edges.tsv"
    ]
    
    for file_name in test_files:
        # Create temp file with sample content
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, file_name)
            with open(file_path, 'w') as f:
                f.write("id\tcategory\ntest:1\ttest_category\n")
            
            # Test the new DuckDB behavior using our util function
            from cat_merge.duckdb_utils import _load_file_list
            conn = duckdb.connect()
            
            if "_nodes" in file_name:
                _load_file_list(conn, "test_nodes", [file_path], "_nodes")
                table_name = "test_nodes"
            else:
                _load_file_list(conn, "test_edges", [file_path], "_edges") 
                table_name = "test_edges"
            
            # Get the provided_by value
            result = conn.execute(f"SELECT DISTINCT provided_by FROM {table_name}").fetchone()[0]
            
            # Compare with pandas behavior
            expected = Path(file_name).stem
            
            print(f"{file_name} → DuckDB: '{result}', Pandas: '{expected}', Match: {result == expected}")
            assert result == expected, f"DuckDB should match pandas: expected '{expected}', got '{result}'"


def test_duckdb_glob_patterns():
    """Test DuckDB glob pattern processing matches pandas."""
    print("\n=== Testing Glob Pattern Processing ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test files
        test_files = [
            "my_source_nodes.tsv",
            "another_data_source_nodes.tsv",
            "simple_edges.tsv", 
            "complex_name_with_underscores_edges.tsv"
        ]
        
        for file_name in test_files:
            file_path = os.path.join(tmpdir, file_name)
            with open(file_path, 'w') as f:
                f.write("id\tcategory\ntest:1\ttest_category\n")
        
        # Test DuckDB glob reading
        from cat_merge.duckdb_utils import read_kg_files
        conn = read_kg_files(source=tmpdir)
        
        # Get provided_by values from nodes
        node_provided_by = conn.execute("SELECT DISTINCT provided_by FROM nodes ORDER BY provided_by").fetchall()
        edge_provided_by = conn.execute("SELECT DISTINCT provided_by FROM edges ORDER BY provided_by").fetchall()
        
        # Expected values (pandas behavior - full stem)
        expected_nodes = ["another_data_source_nodes", "my_source_nodes"]
        expected_edges = ["complex_name_with_underscores_edges", "simple_edges"]
        
        actual_nodes = [row[0] for row in node_provided_by]
        actual_edges = [row[0] for row in edge_provided_by]
        
        print(f"Nodes - Expected: {expected_nodes}, Actual: {actual_nodes}")
        print(f"Edges - Expected: {expected_edges}, Actual: {actual_edges}")
        
        assert actual_nodes == expected_nodes, f"Node provided_by mismatch: expected {expected_nodes}, got {actual_nodes}"
        assert actual_edges == expected_edges, f"Edge provided_by mismatch: expected {expected_edges}, got {actual_edges}"


if __name__ == "__main__":
    test_duckdb_individual_files()
    test_duckdb_glob_patterns()
    print("\n✓ All tests passed! DuckDB now matches pandas behavior.")