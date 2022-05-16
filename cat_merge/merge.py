import typer
import yaml
from typing import List

from cat_merge.qc_utils import create_qc_report
from file_utils import write
from merge_utils import merge_kg


def merge(name: str,
          edges: List[str] = typer.Option(...),
          nodes: List[str] = typer.Option(...),
          output_dir: str = typer.Option("merge-output")):
    kg = merge_kg(node_files=nodes, edge_files=edges)
    write(name=name, kg=kg, output_dir=output_dir)
    qc_report = create_qc_report(kg)

    with open("qc_report.yaml", "w") as report_file:
        yaml.dump(qc_report, report_file)

    print(qc_report)


if __name__ == "__main__":
    typer.run(merge)
