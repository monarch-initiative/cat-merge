import typer
from typing import List


def merge(name: str,
          edge_files: List[str] = typer.Option(...),
          node_files: List[str] = typer.Option(...),
          output_dir: str = typer.Option("merge-output")):
    print(name)
    print(edge_files)
    print(node_files)
    print(output_dir)


if __name__ == "__main__":
    typer.run(merge)
