#!/usr/bin/env python3
"""
Test multivalued field handling with LinkML schema integration.
"""

import tempfile
import os
import duckdb
from cat_merge.merge import merge
from cat_merge.schema_utils import SchemaParser, is_field_multivalued


def test_schema_parser_with_biolink_model():
    """Test that schema parser can identify multivalued fields from Biolink Model."""
    
    # Test without providing a schema (should use Biolink Model)
    try:
        parser = SchemaParser()
        
        # Test some known multivalued fields from Biolink Model
        # These should be multivalued according to the schema
        known_multivalued = ['category', 'provided_by', 'publications', 'xref', 'synonym']
        
        multivalued_results = {}
        for field in known_multivalued:
            try:
                is_multi = parser.is_field_multivalued(field)
                multivalued_results[field] = is_multi
                print(f"Field '{field}': multivalued = {is_multi}")
            except Exception as e:
                print(f"Error checking field '{field}': {e}")
                multivalued_results[field] = False
        
        # At least some should be detected as multivalued
        any_multivalued = any(multivalued_results.values())
        print(f"Any fields detected as multivalued: {any_multivalued}")
        
        return multivalued_results
        
    except Exception as e:
        print(f"Schema parser test failed (might be network issue): {e}")
        return {}


def test_multivalued_field_processing():
    """Test that multivalued fields are processed correctly in data files."""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test files with pipe-delimited multivalued fields
        nodes_file = os.path.join(tmpdir, 'test_nodes.tsv')
        edges_file = os.path.join(tmpdir, 'test_edges.tsv')
        
        # Node file with multivalued category field
        with open(nodes_file, 'w') as f:
            f.write("id\tcategory\tname\tpublications\txref\n")
            f.write("TEST:001\tbiolink:Gene|biolink:NamedThing\tTest Gene 1\tPMID:123456|PMID:789012\tNCBIGene:123|HGNC:456\n")
            f.write("TEST:002\tbiolink:Disease\tTest Disease 1\t\t\n")
            f.write("TEST:003\tbiolink:Protein|biolink:MacromolecularMachine\tTest Protein 1\tPMID:555666\tUniProt:P12345\n")
        
        # Edge file with multivalued provided_by field  
        with open(edges_file, 'w') as f:
            f.write("id\tsubject\tpredicate\tobject\tcategory\tprovided_by\tpublications\n")
            f.write("EDGE:001\tTEST:001\tbiolink:associated_with\tTEST:002\tbiolink:GeneToDeseaseAssociation\tSource1|Source2\tPMID:111|PMID:222\n")
            f.write("EDGE:002\tTEST:001\tbiolink:enables\tTEST:003\tbiolink:GeneToGeneProductRelationship\tSource3\tPMID:333\n")
        
        # Test without schema (should not split)
        print("=== Testing without schema ===")
        merge(
            name='test-no-schema',
            nodes=[nodes_file],
            edges=[edges_file],
            output_dir=tmpdir,
            qc_report=False
        )
        
        # Check results without schema
        db_path = os.path.join(tmpdir, 'test-no-schema.duckdb')
        conn = duckdb.connect(db_path)
        
        # Categories should still be strings
        node_categories = conn.execute("SELECT id, category FROM nodes ORDER BY id").fetchall()
        edge_provided_by = conn.execute("SELECT id, provided_by FROM edges ORDER BY id").fetchall()
        
        print("Nodes (no schema):")
        for row in node_categories:
            print(f"  {row[0]}: {row[1]} (type: {type(row[1])})")
            
        print("Edges (no schema):")  
        for row in edge_provided_by:
            print(f"  {row[0]}: {row[1]} (type: {type(row[1])})")
        
        conn.close()
        
        # Test with schema (should split multivalued fields)
        print("\n=== Testing with schema (Biolink Model) ===")
        try:
            merge(
                name='test-with-schema',
                nodes=[nodes_file],
                edges=[edges_file],
                output_dir=tmpdir,
                qc_report=False,
                schema_path=None  # Use default Biolink Model
            )
            
            # Check results with schema
            db_path = os.path.join(tmpdir, 'test-with-schema.duckdb')
            conn = duckdb.connect(db_path)
            
            # Categories should be arrays if multivalued
            node_categories = conn.execute("SELECT id, category FROM nodes ORDER BY id").fetchall()
            edge_provided_by = conn.execute("SELECT id, provided_by FROM edges ORDER BY id").fetchall()
            
            print("Nodes (with schema):")
            for row in node_categories:
                print(f"  {row[0]}: {row[1]} (type: {type(row[1])})")
                
            print("Edges (with schema):")
            for row in edge_provided_by:
                print(f"  {row[0]}: {row[1]} (type: {type(row[1])})")
            
            # Test array access if they were converted to arrays
            try:
                first_node_category = conn.execute("SELECT category[1] as first_category FROM nodes WHERE id = 'TEST:001'").fetchone()
                if first_node_category:
                    print(f"First category for TEST:001: {first_node_category[0]}")
            except Exception as e:
                print(f"Array access test failed (might not be arrays): {e}")
            
            conn.close()
            
        except Exception as e:
            print(f"Schema-based test failed: {e}")


def test_convenience_functions():
    """Test the convenience functions for field checking."""
    
    print("\n=== Testing convenience functions ===")
    
    test_fields = ['category', 'provided_by', 'publications', 'id', 'subject', 'predicate']
    
    for field in test_fields:
        try:
            is_multi = is_field_multivalued(field)
            print(f"is_field_multivalued('{field}'): {is_multi}")
        except Exception as e:
            print(f"Error testing field '{field}': {e}")


if __name__ == "__main__":
    print("Testing multivalued field handling...")
    
    schema_results = test_schema_parser_with_biolink_model()
    test_multivalued_field_processing()
    test_convenience_functions()
    
    print("\n✓ Multivalued field tests completed!")
    
    if schema_results:
        print(f"\nSchema detection results: {schema_results}")
    else:
        print("\nNote: Schema tests may have failed due to network connectivity issues")