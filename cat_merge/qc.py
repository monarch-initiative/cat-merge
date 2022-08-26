import yaml
import logging

from cat_merge.file_utils import read_kg
from cat_merge.qc_utils import create_qc_report, create_stats_report, load_graph

from grape import Graph  # type: ignore


def qc_report(archive_path: str,
              output_dir: str,
              output_name: str = "qc_report.yaml",
              add_provided_by: bool = True,
              # dangling_edges: bool = True,
              # dangling_edges_path: str = None,
              nodes_file_name: str = None,
              edges_file_name: str = None):
    kg = read_kg(archive_path)
    report = create_qc_report(kg)
    del kg

    if nodes_file_name is None or edges_file_name is None:
        pass
    else:
        g = load_graph(name="test_name",
                       version="0.1",
                       edges_path=edges_file_name,
                       nodes_path=nodes_file_name)
        report["stats"] = create_stats_report(g)

    with open(f"{output_dir}/{output_name}", "w") as report_file:
        yaml.dump(report, report_file)
