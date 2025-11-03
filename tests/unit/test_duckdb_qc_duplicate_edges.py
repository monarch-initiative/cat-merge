import pytest
import duckdb
from cat_merge.duckdb_qc import create_qc_report_duckdb


def test_duckdb_qc_report_with_duplicate_edges():
    """Test that the DuckDB QC report correctly reports duplicate edges with predicate breakdown."""

    # Create an in-memory DuckDB connection for testing
    conn = duckdb.connect(":memory:")

    # Create required tables
    conn.execute("""
        CREATE TABLE node_stats (
            provided_by VARCHAR,
            category VARCHAR,
            namespace VARCHAR,
            count INTEGER
        )
    """)

    conn.execute("""
        INSERT INTO node_stats VALUES
        ('test_source', 'Gene', 'HGNC', 2),
        ('test_source', 'Disease', 'MONDO', 1)
    """)

    conn.execute("""
        CREATE TABLE edge_stats (
            provided_by VARCHAR,
            predicate VARCHAR,
            edge_category VARCHAR,
            subject_namespace VARCHAR,
            object_namespace VARCHAR,
            subject_category VARCHAR,
            object_category VARCHAR,
            count INTEGER
        )
    """)

    conn.execute("""
        INSERT INTO edge_stats VALUES
        ('test_source', 'biolink:expressed_in', 'biolink:GeneToExpressionAssoc', 'HGNC', 'UBERON', 'Gene', 'AnatomicalEntity', 3)
    """)

    # Create nodes table - this is crucial for the JOIN in _get_qc_predicates_report
    conn.execute("CREATE TABLE nodes (id VARCHAR, category VARCHAR, provided_by VARCHAR)")
    conn.execute("""
        INSERT INTO nodes VALUES
        ('HGNC:1', 'biolink:Gene', 'test_source'),
        ('UBERON:1', 'biolink:AnatomicalEntity', 'test_source'),
        ('UBERON:2', 'biolink:AnatomicalEntity', 'test_source')
    """)

    # Create duplicate edges table with test data
    # This is the key - we need duplicate edges that will trigger the predicate report
    conn.execute("CREATE TABLE duplicate_edges (id VARCHAR, subject VARCHAR, object VARCHAR, predicate VARCHAR, category VARCHAR, provided_by VARCHAR)")
    conn.execute("""
        INSERT INTO duplicate_edges VALUES
        ('edge:1', 'HGNC:1', 'UBERON:1', 'biolink:expressed_in', 'biolink:GeneToExpressionAssoc', 'test_source'),
        ('edge:2', 'HGNC:1', 'UBERON:2', 'biolink:expressed_in', 'biolink:GeneToExpressionAssoc', 'test_source')
    """)

    # Create empty QC tables
    conn.execute("CREATE TABLE dangling_edges (id VARCHAR, subject VARCHAR, object VARCHAR, predicate VARCHAR, category VARCHAR, provided_by VARCHAR)")
    conn.execute("CREATE TABLE duplicate_nodes (id VARCHAR, category VARCHAR, provided_by VARCHAR)")

    # This should trigger the GROUP BY error in _get_qc_predicates_report
    # when it tries to generate the predicate breakdown for duplicate edges
    qc_report = create_qc_report_duckdb(conn)

    # Verify duplicate edges are reported correctly
    assert 'duplicate_edges' in qc_report
    assert isinstance(qc_report['duplicate_edges'], list)
    assert len(qc_report['duplicate_edges']) == 1  # Should have one source

    duplicate_report = qc_report['duplicate_edges'][0]
    assert duplicate_report['name'] == 'test_source'
    assert duplicate_report['total_number'] == 2  # 2 duplicate edges
    assert isinstance(duplicate_report['categories'], list)
    assert isinstance(duplicate_report['namespaces'], list)
    assert isinstance(duplicate_report['predicates'], list)

    # Verify the predicate report is generated correctly
    assert len(duplicate_report['predicates']) == 1
    predicate_report = duplicate_report['predicates'][0]
    assert predicate_report['name'] == 'biolink:expressed_in'
    assert predicate_report['total_number'] == 2
    assert 'categories' in predicate_report
    assert isinstance(predicate_report['categories'], list)

    conn.close()
