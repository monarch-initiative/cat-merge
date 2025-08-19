import duckdb
import os
from typing import List, Optional, Union
from pathlib import Path


def read_kg_files(
    source: str = None,
    nodes: List[str] = None,
    edges: List[str] = None,
    nodes_match: str = "_nodes",
    edges_match: str = "_edges"
) -> duckdb.DuckDBPyConnection:
    """
    Read knowledge graph files into DuckDB tables.
    
    Args:
        source: Directory path containing node and edge files
        nodes: List of node file paths
        edges: List of edge file paths
        nodes_match: String pattern to match node files
        edges_match: String pattern to match edge files
        
    Returns:
        DuckDB connection with 'nodes' and 'edges' tables loaded
    """
    conn = duckdb.connect()
    
    if source is not None:
        if nodes or edges:
            raise ValueError("Cannot specify both source directory and individual file lists")
        
        if not os.path.isdir(source):
            raise ValueError(f"Source path {source} is not a directory")
            
        # Use glob patterns to read files with automatic source tracking
        nodes_pattern = f"{source}/*{nodes_match}.tsv"
        edges_pattern = f"{source}/*{edges_match}.tsv"
        
        # Create nodes table with provided_by from filename, excluding any input provided_by
        conn.execute(f"""
            CREATE TEMP TABLE temp_nodes AS
            SELECT *
            FROM read_csv_auto('{nodes_pattern}', 
                              filename=true,
                              delim='\t',
                              quote='',
                              all_varchar=true,
                              union_by_name=true,
                              ignore_errors=true)
        """)
        
        # Check if provided_by column exists and create appropriate SELECT
        has_provided_by = conn.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'temp_nodes' AND column_name = 'provided_by'").fetchone()[0] > 0
        exclude_clause = "filename, provided_by" if has_provided_by else "filename"
        
        conn.execute(f"""
            CREATE TABLE nodes AS
            SELECT 
                * EXCLUDE ({exclude_clause}),
                regexp_extract(filename, '/([^/]+){nodes_match}\.tsv$', 1) as provided_by
            FROM temp_nodes
        """)        
        
        # Create edges table with provided_by from filename, excluding any input provided_by
        conn.execute(f"""
            CREATE TEMP TABLE temp_edges AS
            SELECT *
            FROM read_csv_auto('{edges_pattern}',
                              filename=true, 
                              delim='\t',
                              quote='',
                              all_varchar=true,
                              union_by_name=true,
                              ignore_errors=true)
        """)
        
        # Check if provided_by column exists and create appropriate SELECT
        has_provided_by = conn.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'temp_edges' AND column_name = 'provided_by'").fetchone()[0] > 0
        exclude_clause = "filename, provided_by" if has_provided_by else "filename"
        
        conn.execute(f"""
            CREATE TABLE edges AS
            SELECT 
                * EXCLUDE ({exclude_clause}),
                regexp_extract(filename, '/([^/]+){edges_match}\.tsv$', 1) as provided_by
            FROM temp_edges
        """)
        
    elif nodes and edges:
        # Read individual file lists
        _load_file_list(conn, "nodes", nodes, nodes_match)
        _load_file_list(conn, "edges", edges, edges_match)
        
    else:
        raise ValueError("Must specify either source directory or both nodes and edges file lists")
    
    return conn


def _load_file_list(conn: duckdb.DuckDBPyConnection, table_name: str, files: List[str], match_pattern: str):
    """Load a list of files into a single table with provided_by column."""
    
    # Build UNION ALL query for all files with their provided_by values
    union_parts = []
    for file_path in files:
        provided_by = Path(file_path).stem.replace(match_pattern, "")
        
        # Create temp table to check for provided_by column
        temp_table = f"temp_{table_name}_{len(union_parts)}"
        conn.execute(f"""
            CREATE TEMP TABLE {temp_table} AS
            SELECT *
            FROM read_csv_auto('{file_path}',
                              delim='\t',
                              quote='',
                              all_varchar=true,
                              union_by_name=true,
                              ignore_errors=true)
        """)
        
        # Check if provided_by column exists
        has_provided_by = conn.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{temp_table}' AND column_name = 'provided_by'").fetchone()[0] > 0
        exclude_clause = "provided_by" if has_provided_by else ""
        
        if exclude_clause:
            union_parts.append(f"""
                SELECT * EXCLUDE ({exclude_clause}), '{provided_by}' as provided_by
                FROM {temp_table}
            """)
        else:
            union_parts.append(f"""
                SELECT *, '{provided_by}' as provided_by
                FROM {temp_table}
            """)
    
    # Create table with UNION ALL of all files
    union_query = " UNION ALL ".join(union_parts)
    conn.execute(f"""
        CREATE TABLE {table_name} AS
        {union_query}
    """)


def read_mapping_files(conn: duckdb.DuckDBPyConnection, mappings: List[str]) -> None:
    """
    Read SSSOM mapping files into DuckDB with filename tracking.
    
    Args:
        conn: DuckDB connection
        mappings: List of mapping file paths or glob patterns
    """
    if not mappings:
        return
        
    # Build UNION ALL query for all mapping files with filename tracking
    union_parts = []
    for i, file_pattern in enumerate(mappings):
        # Check if it's a glob pattern or individual file
        if '*' in file_pattern or '?' in file_pattern:
            # Create temp table to check schema
            temp_table = f"temp_mapping_{i}"
            conn.execute(f"""
                CREATE TEMP TABLE {temp_table} AS
                SELECT *
                FROM read_csv_auto('{file_pattern}',
                                  filename=true,
                                  delim='\t',
                                  quote='',
                                  all_varchar=true,
                                  union_by_name=true,
                                  comment='#',
                                  ignore_errors=true)
            """)
            
            # Check if provided_by column exists and create appropriate SELECT
            has_provided_by = conn.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{temp_table}' AND column_name = 'provided_by'").fetchone()[0] > 0
            exclude_clause = "filename, provided_by" if has_provided_by else "filename"
            
            union_parts.append(f"""
                SELECT * EXCLUDE ({exclude_clause}), 
                       regexp_extract(filename, '/([^/]+)\.sssom\.tsv$', 1) as mapping_source
                FROM {temp_table}
            """)
        else:
            # Individual file with source name from filename
            temp_table = f"temp_mapping_{i}"
            conn.execute(f"""
                CREATE TEMP TABLE {temp_table} AS
                SELECT *
                FROM read_csv_auto('{file_pattern}',
                                  delim='\t',
                                  quote='',
                                  all_varchar=true,
                                  union_by_name=true,
                                  comment='#',
                                  ignore_errors=true)
            """)
            
            # Check if provided_by column exists
            has_provided_by = conn.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{temp_table}' AND column_name = 'provided_by'").fetchone()[0] > 0
            exclude_clause = "provided_by" if has_provided_by else ""
            
            file_stem = Path(file_pattern).stem
            if exclude_clause:
                union_parts.append(f"""
                    SELECT * EXCLUDE ({exclude_clause}), '{file_stem}' as mapping_source
                    FROM {temp_table}
                """)
            else:
                union_parts.append(f"""
                    SELECT *, '{file_stem}' as mapping_source
                    FROM {temp_table}
                """)
    
    # Create table with UNION ALL of all mapping files
    union_query = " UNION ALL ".join(union_parts)
    conn.execute(f"""
        CREATE TABLE mappings AS
        {union_query}
    """)


def apply_mappings(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Apply SSSOM mappings to edges table.
    
    Args:
        conn: DuckDB connection with edges and mappings tables
    """
    conn.execute("""
        -- Create temporary table with original subject/object columns
        CREATE TEMP TABLE edges_with_mappings AS
        SELECT 
            e.*,
            e.subject as original_subject,
            e.object as original_object,
            sm_subj.subject_id as mapped_subject,
            sm_obj.subject_id as mapped_object
        FROM edges e
        LEFT JOIN mappings sm_subj ON e.subject = sm_subj.object_id
        LEFT JOIN mappings sm_obj ON e.object = sm_obj.object_id;
        
        -- Update edges table with mappings applied, preserving all original columns
        DROP TABLE edges;
        CREATE TABLE edges AS
        SELECT 
            * EXCLUDE (subject, object, original_subject, original_object, mapped_subject, mapped_object),
            COALESCE(mapped_subject, original_subject) as subject,
            COALESCE(mapped_object, original_object) as object,
            CASE WHEN mapped_subject IS NOT NULL AND mapped_subject != original_subject 
                 THEN original_subject ELSE NULL END as original_subject,
            CASE WHEN mapped_object IS NOT NULL AND mapped_object != original_object
                 THEN original_object ELSE NULL END as original_object
        FROM edges_with_mappings;
    """)


def merge_and_clean(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Perform merge operations: deduplicate nodes and edges, identify QC issues.
    
    Args:
        conn: DuckDB connection with nodes and edges tables
    """
    
    # Create QC tables for tracking issues before cleaning
    conn.execute("""
        -- Duplicate nodes (before cleaning)
        CREATE TABLE duplicate_nodes AS
        SELECT * FROM nodes 
        WHERE id IN (
            SELECT id FROM nodes 
            GROUP BY id 
            HAVING COUNT(*) > 1
        );
    """)
    
    conn.execute("""
        -- Duplicate edges (before cleaning) 
        CREATE TABLE duplicate_edges AS
        SELECT * FROM edges
        WHERE id IN (
            SELECT id FROM edges
            GROUP BY id 
            HAVING COUNT(*) > 1
        );
    """)
    
    conn.execute("""
        -- Dangling edges (edges referencing non-existent nodes)
        CREATE TABLE dangling_edges AS
        SELECT e.* FROM edges e
        WHERE e.subject NOT IN (SELECT DISTINCT id FROM nodes)
           OR e.object NOT IN (SELECT DISTINCT id FROM nodes);
    """)
    
    # Clean nodes - remove duplicates, keep first occurrence
    conn.execute("""
        CREATE TEMP TABLE clean_nodes AS
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY provided_by) as rn
            FROM nodes
        ) WHERE rn = 1;
        
        DROP TABLE nodes;
        CREATE TABLE nodes AS SELECT * FROM clean_nodes;
        ALTER TABLE nodes DROP COLUMN rn;
    """)
    
    # Clean edges - remove duplicates and dangling edges
    conn.execute("""
        CREATE TEMP TABLE clean_edges AS
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY provided_by) as rn
            FROM edges e
            WHERE e.subject IN (SELECT DISTINCT id FROM nodes)
              AND e.object IN (SELECT DISTINCT id FROM nodes)
        ) WHERE rn = 1;
        
        DROP TABLE edges;
        CREATE TABLE edges AS SELECT * FROM clean_edges;
        ALTER TABLE edges DROP COLUMN rn;
    """)


def create_qc_aggregations(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create pre-aggregated tables for QC reporting.
    
    Args:
        conn: DuckDB connection with clean nodes and edges tables
    """
    
    # Node statistics aggregation
    conn.execute("""
        CREATE TABLE node_stats AS
        SELECT 
            provided_by,
            category,
            split_part(id, ':', 1) as namespace,
            COUNT(*) as count
        FROM nodes
        GROUP BY provided_by, category, namespace;
    """)
    
    # Edge statistics aggregation with subject/object categories from joins
    conn.execute("""
        CREATE TABLE edge_stats AS
        SELECT 
            e.provided_by,
            e.category as edge_category,
            e.predicate,
            split_part(e.subject, ':', 1) as subject_namespace,
            split_part(e.object, ':', 1) as object_namespace,
            sn.category as subject_category,
            on_node.category as object_category,
            COUNT(*) as count
        FROM edges e
        LEFT JOIN nodes sn ON e.subject = sn.id  
        LEFT JOIN nodes on_node ON e.object = on_node.id
        GROUP BY 
            e.provided_by, 
            e.category, 
            e.predicate,
            subject_namespace, 
            object_namespace,
            sn.category,
            on_node.category;
    """)


def write_output_files(conn: duckdb.DuckDBPyConnection, name: str, output_dir: str) -> None:
    """
    Write cleaned nodes and edges to TSV files.
    
    Args:
        conn: DuckDB connection
        name: Output file prefix  
        output_dir: Output directory path
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(f"{output_dir}/qc", exist_ok=True)
    
    # Write main output files
    conn.execute(f"""
        COPY nodes TO '{output_dir}/{name}_nodes.tsv' 
        WITH (FORMAT CSV, DELIMITER '\t', HEADER);
    """)
    
    conn.execute(f"""
        COPY edges TO '{output_dir}/{name}_edges.tsv'
        WITH (FORMAT CSV, DELIMITER '\t', HEADER);
    """)
    
    # Write QC files
    conn.execute(f"""
        COPY duplicate_nodes TO '{output_dir}/qc/{name}-duplicate-nodes.tsv'
        WITH (FORMAT CSV, DELIMITER '\t', HEADER);
    """)
    
    conn.execute(f"""
        COPY duplicate_edges TO '{output_dir}/qc/{name}-duplicate-edges.tsv'
        WITH (FORMAT CSV, DELIMITER '\t', HEADER);
    """)
    
    conn.execute(f"""
        COPY dangling_edges TO '{output_dir}/qc/{name}-dangling-edges.tsv'
        WITH (FORMAT CSV, DELIMITER '\t', HEADER);
    """)