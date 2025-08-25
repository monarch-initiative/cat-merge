import click
from cat_merge.merge import merge as pandas_merge
from cat_merge.duckdb_merge import merge_duckdb


@click.command()
@click.option('--name', help='Name of KG to merge')
@click.option('--source', help='Optional directory containing node and edge files')
@click.option('--mapping', multiple=True, required=False, help='Optional SSSOM mapping file(s) or glob patterns (e.g., mappings/*.sssom.tsv)')
@click.option('--output_dir', help='Directory to output knowledge graph')
@click.option('--qc_report', required=False, default=True,
              help='Boolean for whether to generate a qc report (defaults to True')
@click.option('--graph_stats', required=False, default=False,
              help='Boolean for whether to generate comprehensive graph statistics report (defaults to False)')
@click.option('--engine', type=click.Choice(['pandas', 'duckdb']), default='duckdb',
              help='Merge engine to use: duckdb (default, faster) or pandas for compatibility')
@click.option('--schema', help='Optional path to LinkML schema file for multivalued field detection')
def main(name, source, mapping, output_dir, qc_report, graph_stats, engine, schema):
    """
    Command line interface for merging nodes and edges into a knowledge graph.

    Args:
        name (str): Output name of KG after merge.
        source (str, optional): Directory or archive containing node and edge files.
        mapping (str or list[str], optional): SSSOM mapping file(s).
        output_dir (str): Directory to output knowledge graph.
        qc_report (bool, optional): Boolean for whether to generate a qc report (defaults to True).
        graph_stats (bool, optional): Boolean for whether to generate comprehensive graph statistics report (defaults to False).
        engine (str): Merge engine to use (pandas or duckdb).

    Returns:
        None
    """
    if engine == 'pandas':
        pandas_merge(name=name, source=source, mappings=mapping, output_dir=output_dir, qc_report=qc_report)
    else:
        merge_duckdb(name=name, source=source, mappings=mapping, output_dir=output_dir, qc_report=qc_report, graph_stats=graph_stats, schema_path=schema)


if __name__ == "__main__":
    main()
