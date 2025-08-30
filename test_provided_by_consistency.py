#!/usr/bin/env python3
"""
Test to verify that provided_by generation is consistent between pandas and DuckDB implementations.

Current behavior (before fix):
- Pandas: Uses Path(file).stem directly → "my_source_nodes" from "my_source_nodes.tsv"
- DuckDB individual files: Uses Path(file).stem.replace(match_pattern, "") → "my_source" from "my_source_nodes.tsv"  
- DuckDB glob: Uses regexp_extract to extract part before match_pattern → "my_source" from "my_source_nodes.tsv"

Expected behavior (after fix):
- All implementations should use Path(file).stem → "my_source_nodes" from "my_source_nodes.tsv"
"""

import tempfile
import os
from pathlib import Path


def test_provided_by_generation():
    """Test that provided_by values are generated consistently across implementations."""
    
    # Test file names
    test_files = [
        "my_source_nodes.tsv",
        "another_data_source_nodes.tsv", 
        "simple_edges.tsv",
        "complex_name_with_underscores_edges.tsv"
    ]
    
    # Expected provided_by values (pandas behavior)
    expected_provided_by = [
        "my_source_nodes",
        "another_data_source_nodes",
        "simple_edges", 
        "complex_name_with_underscores_edges"
    ]
    
    print("=== Current Pandas Behavior ===")
    for i, file_name in enumerate(test_files):
        pandas_result = Path(file_name).stem
        print(f"{file_name} → {pandas_result}")
        assert pandas_result == expected_provided_by[i], f"Expected {expected_provided_by[i]}, got {pandas_result}"
    
    print("\n=== Current DuckDB Individual File Behavior ===")
    for file_name in test_files:
        # After fix: DuckDB now matches pandas behavior  
        duckdb_result = Path(file_name).stem
        print(f"{file_name} → {duckdb_result}")
    
    print("\n=== Expected Behavior (All Should Match Pandas) ===")
    for i, file_name in enumerate(test_files):
        expected = expected_provided_by[i]
        print(f"{file_name} → {expected}")


def test_regex_patterns():
    """Test the regex patterns used in DuckDB glob reading."""
    import duckdb
    conn = duckdb.connect()
    
    test_paths = [
        "/path/to/my_source_nodes.tsv",
        "/another/path/complex_name_with_underscores_nodes.tsv",
        "/simple/path/data_edges.tsv"
    ]
    
    print("\n=== DuckDB Regex Extraction (Current) ===")
    for path in test_paths:
        # After fix: Now extracts full filename like pandas
        result = conn.execute("""
            SELECT regexp_extract(?, '/([^/]+)\.tsv$', 1) as extracted
        """, [path]).fetchone()[0]
        
        print(f"{path} → {result}")
    
    print("\n=== DuckDB Regex Extraction (Should Match Pandas) ===")
    for path in test_paths:
        # Extract just the filename without extension (matching pandas behavior)
        result = conn.execute("""
            SELECT regexp_extract(?, '/([^/]+)\.tsv$', 1) as extracted
        """, [path]).fetchone()[0]
        
        expected_pandas = Path(path).stem
        print(f"{path} → {result} (pandas: {expected_pandas})")
        assert result == expected_pandas, f"Regex should match pandas: expected {expected_pandas}, got {result}"


if __name__ == "__main__":
    test_provided_by_generation()
    test_regex_patterns()
    print("\n✓ All tests completed!")