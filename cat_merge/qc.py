import yaml
from cat_merge.file_utils import read_kg
from cat_merge.qc_utils import create_qc_report
from cat_merge.qc_diff_utils import diff_yaml


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

    with open(f"{output_dir}/{output_name}", "w") as report_file:
        yaml.dump(report, report_file)


def qc_diff(qc_file_a: str, qc_file_b: str, output_path: str = None, show_all: bool = False):
    with open(qc_file_a, "r") as yml_file:
        qc_yaml_a = yaml.safe_load(yml_file)
    with open(qc_file_b, "r") as yml_file:
        qc_yaml_b = yaml.safe_load(yml_file)

    report = diff_yaml(qc_yaml_a, qc_yaml_b, show_all)

    if output_path is not None:
        with open(output_path, "w") as report_file:
            yaml.dump(report, report_file)

    return report
