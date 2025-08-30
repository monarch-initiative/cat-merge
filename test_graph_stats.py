#!/usr/bin/env python3
"""
Test graph statistics report generation.
"""

import tempfile
import os
import yaml
from cat_merge.duckdb_merge import merge_duckdb


def test_graph_stats_report():
    """Test that graph statistics report is generated correctly."""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create simple test files
        nodes_file = os.path.join(tmpdir, 'test_nodes.tsv')
        edges_file = os.path.join(tmpdir, 'test_edges.tsv')
        
        # Node file with diverse categories and prefixes
        with open(nodes_file, 'w') as f:
            f.write("id\tcategory\tname\n")
            f.write("HGNC:123\tbiolink:Gene\tTest Gene 1\n")
            f.write("HGNC:456\tbiolink:Gene\tTest Gene 2\n")
            f.write("MONDO:001\tbiolink:Disease\tTest Disease 1\n")
            f.write("HP:001\tbiolink:PhenotypicFeature\tTest Phenotype 1\n")
            f.write("HP:002\tbiolink:PhenotypicFeature\tTest Phenotype 2\n")
        
        # Edge file with different predicates
        with open(edges_file, 'w') as f:
            f.write("id\tsubject\tpredicate\tobject\tcategory\n")
            f.write("EDGE:001\tHGNC:123\tbiolink:causes\tMONDO:001\tbiolink:GeneToDiseaseAssociation\n")
            f.write("EDGE:002\tMONDO:001\tbiolink:has_phenotype\tHP:001\tbiolink:DiseaseToPhenotypicFeatureAssociation\n")
            f.write("EDGE:003\tHGNC:456\tbiolink:interacts_with\tHGNC:123\tbiolink:GeneToGeneAssociation\n")
            f.write("EDGE:004\tMONDO:001\tbiolink:has_phenotype\tHP:002\tbiolink:DiseaseToPhenotypicFeatureAssociation\n")
        
        # Run merge with graph stats enabled
        print("Running merge with graph stats enabled...")
        merge_duckdb(
            name='test-graph-stats',
            nodes=[nodes_file],
            edges=[edges_file],
            output_dir=tmpdir,
            qc_report=False,
            graph_stats=True
        )
        
        # Check that graph stats file was created
        stats_file = os.path.join(tmpdir, 'merged_graph_stats.yaml')
        assert os.path.exists(stats_file), "Graph stats file was not created"
        
        # Load and examine the stats
        with open(stats_file, 'r') as f:
            stats_data = yaml.safe_load(f)
        
        print(f"Graph stats file created successfully: {stats_file}")
        
        # Verify basic structure
        assert 'node_stats' in stats_data, "Missing node_stats section"
        assert 'edge_stats' in stats_data, "Missing edge_stats section"
        assert 'graph_name' in stats_data, "Missing graph_name"
        
        node_stats = stats_data['node_stats']
        edge_stats = stats_data['edge_stats']
        
        # Check node statistics
        print(f"Total nodes: {node_stats['total_nodes']}")
        assert node_stats['total_nodes'] == 5, f"Expected 5 nodes, got {node_stats['total_nodes']}"
        
        print("Node categories:")
        for category, info in node_stats['count_by_category'].items():
            print(f"  {category}: {info['count']}")
        
        # Verify expected categories
        expected_node_categories = ['biolink:Gene', 'biolink:Disease', 'biolink:PhenotypicFeature']
        for cat in expected_node_categories:
            assert cat in node_stats['count_by_category'], f"Missing node category: {cat}"
        
        print("Node ID prefixes:")
        for prefix, count in node_stats['count_by_id_prefixes'].items():
            print(f"  {prefix}: {count}")
        
        # Verify expected prefixes
        expected_prefixes = ['HGNC', 'MONDO', 'HP']
        for prefix in expected_prefixes:
            assert prefix in node_stats['count_by_id_prefixes'], f"Missing node prefix: {prefix}"
        
        # Check edge statistics  
        print(f"Total edges: {edge_stats['total_edges']}")
        assert edge_stats['total_edges'] == 4, f"Expected 4 edges, got {edge_stats['total_edges']}"
        
        print("Edge predicates:")
        for predicate, info in edge_stats['count_by_predicates'].items():
            print(f"  {predicate}: {info['count']}")
        
        # Verify expected predicates
        expected_predicates = ['biolink:causes', 'biolink:has_phenotype', 'biolink:interacts_with']
        for pred in expected_predicates:
            assert pred in edge_stats['count_by_predicates'], f"Missing predicate: {pred}"
        
        print("Sample SPO patterns:")
        spo_count = 0
        for spo_pattern, info in edge_stats['count_by_spo'].items():
            if spo_count < 3:  # Show first 3
                print(f"  {spo_pattern}: {info['count']}")
                spo_count += 1
        
        print("✓ Graph stats report test passed!")
        return True


def test_graph_stats_disabled():
    """Test that graph stats file is not created when disabled (default)."""
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create minimal test files
        nodes_file = os.path.join(tmpdir, 'test_nodes.tsv')
        edges_file = os.path.join(tmpdir, 'test_edges.tsv')
        
        with open(nodes_file, 'w') as f:
            f.write("id\tcategory\tname\n")
            f.write("TEST:001\tbiolink:Gene\tTest Gene\n")
        
        with open(edges_file, 'w') as f:
            f.write("id\tsubject\tpredicate\tobject\tcategory\n")
            f.write("EDGE:001\tTEST:001\tbiolink:related_to\tTEST:001\tbiolink:Association\n")
        
        # Run merge with graph stats disabled (default)
        print("Running merge with graph stats disabled...")
        merge_duckdb(
            name='test-no-stats',
            nodes=[nodes_file],
            edges=[edges_file],
            output_dir=tmpdir,
            qc_report=False,
            graph_stats=False  # Explicitly disabled
        )
        
        # Check that graph stats file was NOT created
        stats_file = os.path.join(tmpdir, 'merged_graph_stats.yaml')
        assert not os.path.exists(stats_file), "Graph stats file should not be created when disabled"
        
        print("✓ Graph stats disabled test passed!")
        return True


if __name__ == "__main__":
    print("Testing graph statistics report generation...")
    
    test_graph_stats_report()
    test_graph_stats_disabled()
    
    print("\n✓ All graph stats tests completed successfully!")