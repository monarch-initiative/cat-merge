import click
from cat_merge.merge import merge as merge


@click.command()
@click.option('--name', help='Name of KG to merge')
@click.option('--input_dir', help='Optional directory containing node and edge files')
@click.option('--mapping', multiple=True, required=False, help='Optional SSSOM mapping file(s)')
@click.option('--output_dir', help='Directory to output knowledge graph')
@click.option('--qc_report', required=False, default=True,
              help='Boolean for whether to generate a qc report (defaults to True')
def main(name, input_dir, mapping, output_dir, qc_report):
    merge(name=name, input_dir=input_dir, mappings=mapping, output_dir=output_dir, qc_report=qc_report)


if __name__ == "__main__":
    main()
