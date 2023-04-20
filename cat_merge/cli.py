import click
from cat_merge.merge import merge as merge


@click.command()
@click.option('--name', help='Name of KG to merge')
@click.option('--source', help='Optional directory containing node and edge files')
@click.option('--mapping', multiple=True, required=False, help='Optional SSSOM mapping file(s)')
@click.option('--output_dir', help='Directory to output knowledge graph')
@click.option('--qc_report', required=False, default=True,
              help='Boolean for whether to generate a qc report (defaults to True')
def main(name, source, mapping, output_dir, qc_report):
    """
    Command line interface for merging nodes and edges into a knowledge graph.

    Args:
        name (str): Output name of KG after merge.
        source (str, optional): Directory or archive containing node and edge files.
        mapping (str or list[str], optional): SSSOM mapping file(s).
        output_dir (str): Directory to output knowledge graph.
        qc_report (bool, optional): Boolean for whether to generate a qc report (defaults to True).

    Returns:
        None
    """
    merge(name=name, source=source, mappings=mapping, output_dir=output_dir, qc_report=qc_report)


if __name__ == "__main__":
    main()
