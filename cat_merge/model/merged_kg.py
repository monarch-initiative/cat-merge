from pandas.core.frame import DataFrame


class MergedKG:
    def __init__(self,
                 nodes: DataFrame,
                 edges: DataFrame,
                 ):
        self.nodes = nodes
        self.edges = edges


class MergeQC:
    def __init__(self, duplicate_nodes: DataFrame, duplicate_edges: DataFrame, dangling_edges: DataFrame):
        self.duplicate_nodes = duplicate_nodes
        self.duplicate_edges = duplicate_edges
        self.dangling_edges = dangling_edges
