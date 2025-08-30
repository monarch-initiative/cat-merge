#!/usr/bin/env python3
"""
Test that persistent DuckDB databases can be run multiple times without "Table already exists" errors.
"""

import tempfile
import os
import duckdb
from cat_merge.merge import merge_duckdb


def test_rerun_persistent_database():
    """Test that running merge multiple times on same database file works."""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, 'test-rerun.duckdb')
        
        # First run - creates the database
        print("=== First run ===")
        merge_duckdb(
            name='test-rerun',
            source='tests/test_data',
            output_dir=tmpdir,
            qc_report=False
        )
        
        # Verify database exists and has data
        assert os.path.exists(db_path), "Database should be created"
        conn = duckdb.connect(db_path)
        first_node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        first_edge_count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        conn.close()
        
        assert first_node_count > 0, "First run should have nodes"
        assert first_edge_count > 0, "First run should have edges"
        
        # Second run - should replace existing tables without error
        print("\n=== Second run (testing table replacement) ===")
        merge_duckdb(
            name='test-rerun',
            source='tests/test_data',
            output_dir=tmpdir,
            qc_report=False
        )
        
        # Verify data is consistent after second run
        conn = duckdb.connect(db_path)
        second_node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        second_edge_count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        
        # Check all expected tables exist
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        table_names = {table[0] for table in tables}
        expected_tables = {'nodes', 'edges', 'node_stats', 'edge_stats', 'duplicate_nodes', 'duplicate_edges', 'dangling_edges'}
        
        conn.close()
        
        # Verify counts are the same (should be deterministic)
        assert second_node_count == first_node_count, f"Node count should be consistent: {first_node_count} vs {second_node_count}"
        assert second_edge_count == first_edge_count, f"Edge count should be consistent: {first_edge_count} vs {second_edge_count}"
        assert expected_tables.issubset(table_names), f"Missing tables: {expected_tables - table_names}"
        
        print(f"✓ Both runs produced same results: {second_node_count} nodes, {second_edge_count} edges")
        print(f"✓ All expected tables present: {sorted(table_names)}")


def test_multiple_reruns():
    """Test running merge 3+ times to ensure robustness."""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        print("\n=== Testing multiple reruns ===")
        
        results = []
        for i in range(3):
            print(f"Run {i+1}/3")
            merge_duckdb(
                name='multi-test',
                source='tests/test_data',
                output_dir=tmpdir,
                qc_report=False
            )
            
            # Check results
            db_path = os.path.join(tmpdir, 'multi-test.duckdb')
            conn = duckdb.connect(db_path)
            node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            edge_count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
            conn.close()
            
            results.append((node_count, edge_count))
        
        # All runs should produce identical results
        assert all(r == results[0] for r in results), f"Inconsistent results across runs: {results}"
        print(f"✓ All 3 runs produced identical results: {results[0]}")


if __name__ == "__main__":
    test_rerun_persistent_database()
    test_multiple_reruns()
    print("\n✓ All persistent database rerun tests passed!")