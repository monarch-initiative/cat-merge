import numpy as np
from pandas.core.frame import DataFrame


def apply_mappings(edges: DataFrame, mapping: DataFrame):

    edges.rename(columns={'subject':'original_subject'}, inplace=True)
    subject_mapping = mapping.rename(columns={'object_id':'subject'})
    subject_mapping = subject_mapping[["subject","subject_id"]]
    edges = edges.merge(subject_mapping, how='left', left_on='original_subject', right_on='subject_id').drop(['subject_id'],axis=1)
    edges['subject'] = np.where(edges.subject.isnull(), edges.original_subject, edges.subject)
    edges['original_subject'] = np.where(edges.subject == edges.original_subject, None, edges.original_subject)

    edges.rename(columns={'object':'original_object'}, inplace=True)
    object_mapping = mapping.rename(columns={'object_id':'object'})
    object_mapping = object_mapping[["object", "subject_id"]]
    edges = edges.merge(object_mapping, how='left', left_on='original_object', right_on='subject_id').drop(['subject_id'],axis=1)
    edges['object'] = np.where(edges.object.isnull(), edges.original_object, edges.object)
    edges['original_object'] = np.where(edges.object == edges.original_object, None, edges.original_object)

    return edges
