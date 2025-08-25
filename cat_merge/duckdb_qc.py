import duckdb
from typing import Dict, List, Union


def create_qc_report_duckdb(conn: duckdb.DuckDBPyConnection) -> Dict:
    """
    Create QC report using pre-aggregated DuckDB tables.
    
    Args:
        conn: DuckDB connection with node_stats and edge_stats tables
        
    Returns:
        Dictionary containing QC report data
    """
    
    # Get basic counts
    total_nodes = conn.execute("SELECT SUM(count) FROM node_stats").fetchone()[0]
    total_edges = conn.execute("SELECT SUM(count) FROM edge_stats").fetchone()[0]
    
    # Get nodes report by provided_by
    nodes_by_source = _get_nodes_report(conn)
    
    # Get edges report by provided_by  
    edges_by_source = _get_edges_report(conn)
    
    return {
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "nodes": nodes_by_source,
        "edges": edges_by_source
    }


def _get_nodes_report(conn: duckdb.DuckDBPyConnection) -> List[Dict]:
    """Create nodes section of QC report."""
    
    # Get provided_by sources
    sources = conn.execute("SELECT DISTINCT provided_by FROM node_stats ORDER BY provided_by").fetchall()
    
    nodes_report = []
    for (source,) in sources:
        # Get stats for this source
        source_stats = conn.execute(f"""
            SELECT 
                SUM(count) as total,
                COUNT(DISTINCT category) as category_count,
                COUNT(DISTINCT namespace) as namespace_count
            FROM node_stats 
            WHERE provided_by = '{source}'
        """).fetchone()
        
        total, category_count, namespace_count = source_stats
        
        # Get categories for this source
        categories = conn.execute(f"""
            SELECT category, SUM(count) as count
            FROM node_stats 
            WHERE provided_by = '{source}'
            GROUP BY category
            ORDER BY category
        """).df()
        
        # Get namespaces for this source
        namespaces = conn.execute(f"""
            SELECT namespace, SUM(count) as count  
            FROM node_stats
            WHERE provided_by = '{source}'
            GROUP BY namespace
            ORDER BY namespace
        """).df()
        
        node_report = {
            "name": source,
            "total_number": total,
            "categories": _df_to_yaml_list(categories, 'category'),
            "namespaces": _df_to_yaml_list(namespaces, 'namespace')
        }
        
        nodes_report.append(node_report)
    
    return nodes_report


def _get_edges_report(conn: duckdb.DuckDBPyConnection) -> List[Dict]:
    """Create edges section of QC report."""
    
    # Get provided_by sources
    sources = conn.execute("SELECT DISTINCT provided_by FROM edge_stats ORDER BY provided_by").fetchall()
    
    edges_report = []
    for (source,) in sources:
        # Get basic stats for this source
        source_stats = conn.execute(f"""
            SELECT 
                SUM(count) as total,
                COUNT(DISTINCT predicate) as predicate_count
            FROM edge_stats 
            WHERE provided_by = '{source}'
        """).fetchone()
        
        total, predicate_count = source_stats
        
        # Get categories for this source (edge categories)
        categories = conn.execute(f"""
            SELECT edge_category as category, SUM(count) as count
            FROM edge_stats 
            WHERE provided_by = '{source}' AND edge_category IS NOT NULL
            GROUP BY edge_category
            ORDER BY edge_category
        """).df()
        
        # Get namespaces (union of subject and object namespaces)
        namespaces = conn.execute(f"""
            SELECT namespace, SUM(count) as count FROM (
                SELECT subject_namespace as namespace, SUM(count) as count 
                FROM edge_stats 
                WHERE provided_by = '{source}' AND subject_namespace IS NOT NULL
                GROUP BY subject_namespace
                UNION ALL
                SELECT object_namespace as namespace, SUM(count) as count 
                FROM edge_stats 
                WHERE provided_by = '{source}' AND object_namespace IS NOT NULL  
                GROUP BY object_namespace
            ) 
            GROUP BY namespace
            ORDER BY namespace
        """).df()
        
        # Get predicates for this source
        predicates = _get_predicates_report(conn, source)
        
        # Get node types (subject/object categories)
        node_types = _get_node_types_report(conn, source)
        
        edge_report = {
            "name": source,
            "total_number": total,
            "categories": _df_to_yaml_list(categories, 'category'),
            "namespaces": _df_to_yaml_list(namespaces, 'namespace'),
            "predicates": predicates,
            "node_types": node_types
        }
        
        edges_report.append(edge_report)
    
    return edges_report


def _get_predicates_report(conn: duckdb.DuckDBPyConnection, source: str) -> List[Dict]:
    """Get predicate breakdown for a source."""
    
    predicates = conn.execute(f"""
        SELECT DISTINCT predicate 
        FROM edge_stats 
        WHERE provided_by = '{source}' AND predicate IS NOT NULL
        ORDER BY predicate
    """).fetchall()
    
    predicate_reports = []
    for (predicate,) in predicates:
        # Get stats for this predicate
        pred_stats = conn.execute(f"""
            SELECT SUM(count) as total
            FROM edge_stats
            WHERE provided_by = '{source}' AND predicate = '{predicate}'
        """).fetchone()
        
        total = pred_stats[0]
        
        # Get subject/object categories for this predicate
        categories = conn.execute(f"""
            SELECT 
                COALESCE(subject_category, 'missing') as subject_category,
                COALESCE(object_category, 'missing') as object_category,
                SUM(count) as count
            FROM edge_stats
            WHERE provided_by = '{source}' AND predicate = '{predicate}'
            GROUP BY subject_category, object_category
            ORDER BY subject_category, object_category
        """).df()
        
        predicate_report = {
            "name": predicate,
            "total_number": total,
            "categories": _df_to_category_pairs(categories)
        }
        
        predicate_reports.append(predicate_report)
    
    return predicate_reports


def _get_node_types_report(conn: duckdb.DuckDBPyConnection, source: str) -> List[Dict]:
    """Get node type breakdown for a source."""
    
    # Get unique subject/object category combinations
    node_type_stats = conn.execute(f"""
        SELECT 
            COALESCE(subject_category, 'missing') as subject_category,
            COALESCE(object_category, 'missing') as object_category,
            SUM(count) as count
        FROM edge_stats
        WHERE provided_by = '{source}'
        GROUP BY subject_category, object_category
        ORDER BY count DESC
    """).df()
    
    return _df_to_category_pairs(node_type_stats)


def _df_to_yaml_list(df, col_name: str) -> List[str]:
    """Convert DataFrame column to list for YAML output."""
    if df.empty:
        return []
    return df[col_name].dropna().astype(str).tolist()


def _df_to_category_pairs(df) -> List[Dict]:
    """Convert DataFrame with subject/object categories to list of dicts."""
    if df.empty:
        return []
    
    result = []
    for _, row in df.iterrows():
        result.append({
            "subject_category": row['subject_category'],
            "object_category": row['object_category'], 
            "count": int(row['count'])
        })
    
    return result


def create_graph_stats_report_duckdb(conn: duckdb.DuckDBPyConnection) -> Dict:
    """
    Create comprehensive graph statistics report similar to merged_graph_stats.yaml.
    
    Args:
        conn: DuckDB connection with nodes and edges tables
        
    Returns:
        Dictionary containing comprehensive graph statistics
    """
    
    # Get total counts
    total_nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    
    # Build the comprehensive report structure
    report = {
        "graph_name": "Graph",
        "node_stats": _generate_node_stats(conn, total_nodes),
        "edge_stats": _generate_edge_stats(conn, total_edges)
    }
    
    return report


def _generate_node_stats(conn: duckdb.DuckDBPyConnection, total_nodes: int) -> Dict:
    """Generate comprehensive node statistics."""
    
    # Get node categories and their counts
    # Handle both string and array categories
    category_counts = conn.execute("""
        SELECT 
            CASE 
                WHEN category IS NULL THEN 'unknown'
                WHEN typeof(category) = 'VARCHAR[]' THEN array_to_string(category, '|')
                ELSE CAST(category AS VARCHAR)
            END as category,
            COUNT(*) as count
        FROM nodes
        GROUP BY category
        ORDER BY category
    """).fetchall()
    
    # Get ID prefixes and their counts
    id_prefix_counts = conn.execute("""
        SELECT 
            split_part(id, ':', 1) as prefix,
            COUNT(*) as count
        FROM nodes
        GROUP BY split_part(id, ':', 1)
        ORDER BY prefix
    """).fetchall()
    
    # Get provided_by sources
    provided_by_sources = conn.execute("""
        SELECT DISTINCT provided_by
        FROM nodes
        ORDER BY provided_by
    """).fetchall()
    
    # Build count_by_category structure
    count_by_category = {}
    for category, count in category_counts:
        # Get provided_by breakdown for this category
        provided_by_breakdown = conn.execute("""
            SELECT provided_by, COUNT(*) as count
            FROM nodes
            WHERE CASE 
                WHEN category IS NULL THEN 'unknown'
                WHEN typeof(category) = 'VARCHAR[]' THEN array_to_string(category, '|')
                ELSE CAST(category AS VARCHAR)
            END = ?
            GROUP BY provided_by
            ORDER BY provided_by
        """, [category]).fetchall()
        
        count_by_category[category] = {
            "count": count,
            "provided_by": {pb: {"count": c} for pb, c in provided_by_breakdown}
        }
    
    # Build count_by_id_prefixes structure
    count_by_id_prefixes = {prefix: count for prefix, count in id_prefix_counts}
    
    # Build count_by_id_prefixes_by_category
    count_by_id_prefixes_by_category = {}
    for category, _ in category_counts:
        prefix_counts = conn.execute("""
            SELECT 
                split_part(id, ':', 1) as prefix,
                COUNT(*) as count
            FROM nodes
            WHERE CASE 
                WHEN category IS NULL THEN 'unknown'
                WHEN typeof(category) = 'VARCHAR[]' THEN array_to_string(category, '|')
                ELSE CAST(category AS VARCHAR)
            END = ?
            GROUP BY split_part(id, ':', 1)
            ORDER BY prefix
        """, [category]).fetchall()
        
        if prefix_counts:
            count_by_id_prefixes_by_category[category] = {prefix: count for prefix, count in prefix_counts}
    
    return {
        "total_nodes": total_nodes,
        "count_by_category": count_by_category,
        "count_by_id_prefixes": count_by_id_prefixes,
        "count_by_id_prefixes_by_category": count_by_id_prefixes_by_category,
        "node_categories": [cat for cat, _ in category_counts],
        "node_id_prefixes": [prefix for prefix, _ in id_prefix_counts],
        "node_id_prefixes_by_category": {
            category: list({prefix: count for prefix, count in conn.execute("""
                SELECT split_part(id, ':', 1) as prefix, COUNT(*) as count
                FROM nodes WHERE CASE 
                    WHEN category IS NULL THEN 'unknown'
                    WHEN typeof(category) = 'VARCHAR[]' THEN array_to_string(category, '|')
                    ELSE CAST(category AS VARCHAR)
                END = ?
                GROUP BY split_part(id, ':', 1)
                ORDER BY count DESC
            """, [category]).fetchall()}.keys())
            for category, _ in category_counts
        },
        "provided_by": [pb[0] for pb in provided_by_sources]
    }


def _generate_edge_stats(conn: duckdb.DuckDBPyConnection, total_edges: int) -> Dict:
    """Generate comprehensive edge statistics."""
    
    # Get all predicates
    predicate_counts = conn.execute("""
        SELECT 
            COALESCE(predicate, 'unknown') as predicate,
            COUNT(*) as count
        FROM edges
        GROUP BY predicate
        ORDER BY predicate
    """).fetchall()
    
    # Get all provided_by sources
    provided_by_sources = conn.execute("""
        SELECT DISTINCT provided_by
        FROM edges
        ORDER BY provided_by
    """).fetchall()
    
    # Build count_by_predicates structure
    count_by_predicates = {}
    for predicate, count in predicate_counts:
        # Get provided_by breakdown for this predicate
        provided_by_breakdown = conn.execute("""
            SELECT provided_by, COUNT(*) as count
            FROM edges
            WHERE COALESCE(predicate, 'unknown') = ?
            GROUP BY provided_by
            ORDER BY provided_by
        """, [predicate]).fetchall()
        
        count_by_predicates[predicate] = {
            "count": count,
            "provided_by": {pb: {"count": c} for pb, c in provided_by_breakdown}
        }
    
    # Build count_by_spo (Subject-Predicate-Object patterns)
    spo_counts = conn.execute("""
        SELECT 
            CASE 
                WHEN sn.category IS NULL THEN 'unknown'
                WHEN typeof(sn.category) = 'VARCHAR[]' THEN array_to_string(sn.category, '|')
                ELSE CAST(sn.category AS VARCHAR)
            END as subject_category,
            COALESCE(e.predicate, 'unknown') as predicate,
            CASE 
                WHEN on_node.category IS NULL THEN 'unknown'
                WHEN typeof(on_node.category) = 'VARCHAR[]' THEN array_to_string(on_node.category, '|')
                ELSE CAST(on_node.category AS VARCHAR)
            END as object_category,
            COUNT(*) as count
        FROM edges e
        LEFT JOIN nodes sn ON e.subject = sn.id  
        LEFT JOIN nodes on_node ON e.object = on_node.id
        GROUP BY subject_category, e.predicate, object_category
        HAVING COUNT(*) > 0
        ORDER BY subject_category, e.predicate, object_category
    """).fetchall()
    
    count_by_spo = {}
    for subj_cat, pred, obj_cat, count in spo_counts:
        spo_key = f"{subj_cat}-{pred}-{obj_cat}"
        
        # Get provided_by breakdown for this SPO pattern
        provided_by_breakdown = conn.execute("""
            SELECT e.provided_by, COUNT(*) as count
            FROM edges e
            LEFT JOIN nodes sn ON e.subject = sn.id  
            LEFT JOIN nodes on_node ON e.object = on_node.id
            WHERE CASE 
                WHEN sn.category IS NULL THEN 'unknown'
                WHEN typeof(sn.category) = 'VARCHAR[]' THEN array_to_string(sn.category, '|')
                ELSE CAST(sn.category AS VARCHAR)
            END = ?
              AND COALESCE(e.predicate, 'unknown') = ?
              AND CASE 
                WHEN on_node.category IS NULL THEN 'unknown'
                WHEN typeof(on_node.category) = 'VARCHAR[]' THEN array_to_string(on_node.category, '|')
                ELSE CAST(on_node.category AS VARCHAR)
            END = ?
            GROUP BY e.provided_by
            ORDER BY e.provided_by
        """, [subj_cat, pred, obj_cat]).fetchall()
        
        count_by_spo[spo_key] = {
            "count": count,
            "provided_by": {pb: {"count": c} for pb, c in provided_by_breakdown}
        }
    
    return {
        "total_edges": total_edges,
        "count_by_predicates": count_by_predicates,
        "count_by_spo": count_by_spo,
        "predicates": [pred for pred, _ in predicate_counts],
        "provided_by": [pb[0] for pb in provided_by_sources]
    }