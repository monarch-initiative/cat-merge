import numpy as np
from pandas.core.frame import DataFrame


def apply_mappings(edges: DataFrame, mapping: DataFrame):

    mapping_dict = mapping.set_index('subject_id')['object_id']

    edges['original_subject'] = edges['subject']
    edges['subject'].replace(mapping_dict, inplace=True)
    edges['original_subject'] = np.where(edges.subject == edges.original_subject, None, edges.original_subject)

    edges['original_object'] = edges['object']
    edges['object'].replace(mapping_dict, inplace=True)
    edges['original_object'] = np.where(edges.object == edges.original_object, None, edges.original_object)

    return edges
