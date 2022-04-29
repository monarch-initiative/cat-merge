import typer
from typing import List
from file_utils import write, read_dfs
from merge_utils import merge_kg


def merge(name: str,
          edges: List[str] = typer.Option(...),
          nodes: List[str] = typer.Option(...),
          output_dir: str = typer.Option("merge-output")):
    node_dfs = read_dfs(nodes)
    edge_dfs = read_dfs(edges)
    write(name=name,
          kg=merge_kg(node_dfs=node_dfs, edge_dfs=edge_dfs),
          output_dir=output_dir)


if __name__ == "__main__":
    typer.run(merge)
