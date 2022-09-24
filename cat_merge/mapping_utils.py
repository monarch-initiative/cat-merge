import numpy as np
from pandas.core.frame import DataFrame


def apply_mappings(edges: DataFrame, mapping: DataFrame):

    edges.rename(columns={'subject':'original_subject'}, inplace=True)
    subject_mapping = mapping.rename(columns={'subject_id':'subject'})
    subject_mapping = subject_mapping[["subject","object_id"]]
    edges = edges.merge(subject_mapping, how='left', left_on='original_subject', right_on='object_id').drop(['object_id'],axis=1)
    edges['subject'] = np.where(edges.subject.isnull(), edges.original_subject, edges.subject)
    edges['original_subject'] = np.where(edges.subject == edges.original_subject, None, edges.original_subject)

    edges.rename(columns={'object':'original_object'}, inplace=True)
    object_mapping = mapping.rename(columns={'subject_id':'object'})
    object_mapping = object_mapping[["object", "object_id"]]
    edges = edges.merge(object_mapping, how='left', left_on='original_object', right_on='object_id').drop(['object_id'],axis=1)
    edges['object'] = np.where(edges.object.isnull(), edges.original_object, edges.object)
    edges['original_object'] = np.where(edges.object == edges.original_object, None, edges.original_object)

    return edges
