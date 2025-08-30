#!/usr/bin/env python3
"""
Test that DuckDB merge creates a persistent database file directly (no copying).
"""

import tempfile
import os
import duckdb
from cat_merge.merge import merge_duckdb


def test_database_save():
    """Test that DuckDB merge creates a queryable database file."""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Run DuckDB merge
        merge_duckdb(
            name='test-database',
            source='tests/test_data',
            output_dir=tmpdir,
            qc_report=False
        )
        
        # Check database file exists
        db_path = os.path.join(tmpdir, 'test-database.duckdb')
        assert os.path.exists(db_path), f"Database file should exist at {db_path}"
        
        # Check database file size is reasonable (not empty)
        db_size = os.path.getsize(db_path)
        assert db_size > 1000, f"Database file should be substantial, got {db_size} bytes"
        
        # Test that database is queryable
        conn = duckdb.connect(db_path)
        
        # Check all expected tables exist
        tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()
        table_names = {table[0] for table in tables}
        
        expected_tables = {'nodes', 'edges', 'node_stats', 'edge_stats', 'duplicate_nodes', 'duplicate_edges', 'dangling_edges'}
        assert expected_tables.issubset(table_names), f"Missing tables: {expected_tables - table_names}"
        
        # Check main tables have data
        node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        edge_count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        
        assert node_count > 0, "Nodes table should have data"
        assert edge_count > 0, "Edges table should have data"
        
        # Check data structure is correct
        node_columns = conn.execute("DESCRIBE nodes").fetchall()
        node_column_names = {col[0] for col in node_columns}
        assert 'id' in node_column_names, "Nodes should have id column"
        assert 'provided_by' in node_column_names, "Nodes should have provided_by column"
        
        edge_columns = conn.execute("DESCRIBE edges").fetchall()
        edge_column_names = {col[0] for col in edge_columns}
        assert 'subject' in edge_column_names, "Edges should have subject column"
        assert 'object' in edge_column_names, "Edges should have object column"
        assert 'provided_by' in edge_column_names, "Edges should have provided_by column"
        
        conn.close()
        
        print(f"✓ Database saved successfully: {db_path}")
        print(f"✓ Database size: {db_size:,} bytes")
        print(f"✓ Contains {len(table_names)} tables: {sorted(table_names)}")
        print(f"✓ Nodes: {node_count}, Edges: {edge_count}")


def test_database_content_matches_tsv():
    """Test that database content matches the TSV output files."""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Run DuckDB merge
        merge_duckdb(
            name='content-test',
            source='tests/test_data',
            output_dir=tmpdir,
            qc_report=False
        )
        
        # Connect to database
        db_path = os.path.join(tmpdir, 'content-test.duckdb')
        conn = duckdb.connect(db_path)
        
        # Read TSV files for comparison
        nodes_tsv = os.path.join(tmpdir, 'content-test_nodes.tsv')
        edges_tsv = os.path.join(tmpdir, 'content-test_edges.tsv')
        
        # Count records in TSV files using DuckDB
        tsv_node_count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{nodes_tsv}', delim='\\t', header=true)").fetchone()[0]
        tsv_edge_count = conn.execute(f"SELECT COUNT(*) FROM read_csv_auto('{edges_tsv}', delim='\\t', header=true)").fetchone()[0]
        
        # Count records in database tables
        db_node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        db_edge_count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        
        # Verify counts match
        assert db_node_count == tsv_node_count, f"Node count mismatch: DB={db_node_count}, TSV={tsv_node_count}"
        assert db_edge_count == tsv_edge_count, f"Edge count mismatch: DB={db_edge_count}, TSV={tsv_edge_count}"
        
        conn.close()
        
        print(f"✓ Database content matches TSV files")
        print(f"✓ Nodes: {db_node_count} (DB) = {tsv_node_count} (TSV)")
        print(f"✓ Edges: {db_edge_count} (DB) = {tsv_edge_count} (TSV)")


if __name__ == "__main__":
    test_database_save()
    test_database_content_matches_tsv()
    print("\n✓ All database save tests passed!")