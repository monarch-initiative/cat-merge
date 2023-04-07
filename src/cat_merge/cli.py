from pathlib import Path
from typing import List

from cat_merge.merge import merge

import typer
app = typer.Typer(
    name="cat-merge",
    help="Merge knowledge graphs",
)

@app.command()
def main(
    name: str = typer.Option(None, "--name", "-n", help="Name of KG to merge"),
    input_dir: str = typer.Option(None, "--input_dir", "-i", help="Directory containing node and edge files"),
    mappings: List[str] = typer.Option(None, "--mappings", "-m", help="Optional SSSOM mapping file(s)"),
    output_dir: str = typer.Option("merged-output", "--output_dir", "-o", help="Directory to output knowledge graph"),
    qc_report: bool = typer.Option(True, "--qc_report", "-r", help="Boolean for whether to generate a qc report"),
):
    """Merge knowledge graphs and generate a QC report"""

    args = locals() 
    if name is None:
        raise ValueError("\n\tMust specify a name\n")
    
    if input_dir is None:
        print("\n\tMust specify an input directory\n")
        raise typer.Exit(1)
    else:
        args["source"] = args.pop("input_dir")
    merge(**args)

# @click.command()
# @click.option('--name', help='Name of KG to merge')
# @click.option('--input_dir', help='Optional directory containing node and edge files')
# @click.option('--mapping', multiple=True, required=False, help='Optional SSSOM mapping file(s)')
# @click.option('--output_dir', help='Directory to output knowledge graph')
# @click.option('--qc_report', required=False, default=True,
#               help='Boolean for whether to generate a qc report (defaults to True')
# def main(name, input_dir, mapping, output_dir, qc_report):
#     merge(name=name, input_dir=input_dir, mappings=mapping, output_dir=output_dir, qc_report=qc_report)


# if __name__ == "__main__":
#     main()
