import numpy as np
from pandas.core.frame import DataFrame


def apply_mappings(edges: DataFrame, mapping: DataFrame):

    # Apply subject mappings 
    edges.rename(columns={'subject':'original_subject'}, inplace=True)
    subject_mapping = mapping.rename(columns={'subject_id':'subject'})
    subject_mapping = subject_mapping[["subject","object_id"]]
    edges = edges.merge(subject_mapping, how='left', left_on='original_subject', right_on='object_id').drop(['object_id'],axis=1)
    edges['subject'] = np.where(edges.subject.isnull(), edges.original_subject, edges.subject)
    edges['original_subject'] = np.where(edges.subject == edges.original_subject, None, edges.original_subject)
    edges = swap_columns(edges, 'original_subject', 'subject')

    # Apply object mappings
    edges.rename(columns={'object':'original_object'}, inplace=True) 
    object_mapping = mapping.rename(columns={'subject_id':'object'})
    object_mapping = object_mapping[["object", "object_id"]]
    edges = edges.merge(object_mapping, how='left', left_on='original_object', right_on='object_id').drop(['object_id'],axis=1)
    edges['object'] = np.where(edges.object.isnull(), edges.original_object, edges.object)
    edges['original_object'] = np.where(edges.object == edges.original_object, None, edges.original_object)
    edges = swap_columns(edges, 'original_object', 'object')

    return edges


def swap_columns(df, col1, col2):
    col_list = list(df)
    x, y = col_list.index(col1), col_list.index(col2)
    col_list[y], col_list[x] = col_list[x], col_list[y]
    return df[col_list]