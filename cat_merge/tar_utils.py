import os
import tarfile
from typing import List


def write_tar(tar_path: str, files: List[str], delete_files=True):
    """
    Write a list of files to a tar archive.

    Args:
        tar_path (str): Path to tar archive.
        files (List[str]): List of files.
        delete_files (bool, optional): Delete files after writing to tar archive.

    Returns:
        None
    """
    tar = tarfile.open(tar_path, "w:gz")
    for file in files:
        tar.add(file, arcname=os.path.basename(file))
    tar.close()
    if delete_files:
        for file in files:
            os.remove(file)