import yaml
from cat_merge.file_utils import read_kg, read_qc
from cat_merge.qc_utils import create_qc_report
from cat_merge.qc_diff_utils import diff_yaml


def qc_report(archive_path: str,
              qc_path: str,
              output_dir: str,
              output_name: str = "qc_report.yaml",
              data_type: type = list,
              group_by: str = "provided_by",
              ):
    """
    Generates a qc report for the knowledge graph

    Args:
        archive_path (str): Path to knowledge graph archive
        qc_path (str): Path to qc report
        output_dir (str): Directory to output qc report
        output_name (str, optional): Name of qc report file (defaults to "qc_report.yaml")
        data_type (type, optional): Type of data to use for qc report (defaults to list)
        group_by (str, optional): Attribute to group qc report by (defaults to "provided_by")

    Returns:
        None
    """
    kg = read_kg(archive_path)
    qc = read_qc(qc_path)

    report = create_qc_report(kg, qc, data_type, group_by)

    with open(f"{output_dir}/{output_name}", "w") as report_file:
        yaml.dump(report, report_file)


def qc_diff(qc_file_a: str, qc_file_b: str, output_path: str = None, show_all: bool = False):
    """
    Compares two qc reports and outputs a diff report

    Args:
        qc_file_a (str): Path to first qc report
        qc_file_b (str): Path to second qc report
        output_path (str, optional): Path to output diff report (defaults to None)
        show_all (bool, optional): Boolean for whether to show all attributes in diff report (defaults to False)

    Returns:
        dict: Diff report
    """
    with open(qc_file_a, "r") as yml_file:
        qc_yaml_a = yaml.safe_load(yml_file)
    with open(qc_file_b, "r") as yml_file:
        qc_yaml_b = yaml.safe_load(yml_file)

    report = diff_yaml(qc_yaml_a, qc_yaml_b, show_all)

    if output_path is not None:
        with open(output_path, "w") as report_file:
            yaml.dump(report, report_file)

    return report
