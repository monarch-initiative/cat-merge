import typer
from typing import List
from file_utils import write
from merge_utils import merge_kg


def merge(name: str,
          edges: List[str] = typer.Option(...),
          nodes: List[str] = typer.Option(...),
          output_dir: str = typer.Option("merge-output")):
    write(name=name, kg=merge_kg(node_files=nodes, edge_files=edges), output_dir=output_dir)


if __name__ == "__main__":
    typer.run(merge)
