import yaml
import logging

from cat_merge.file_utils import read_kg
from cat_merge.qc_utils import create_qc_report

def qc_report(archive_path: str,
              output_dir: str,
              output_name: str = "qc_report.yaml",
              add_provided_by: bool = True,
              # dangling_edges: bool = True,
              # dangling_edges_path: str = None,
              nodes_file_name: str = None,
              edges_file_name: str = None):
    kg = read_kg(archive_path)

    qc_report = create_qc_report(kg)

    with open(f"{output_dir}/{output_name}", "w") as report_file:
        yaml.dump(qc_report, report_file)