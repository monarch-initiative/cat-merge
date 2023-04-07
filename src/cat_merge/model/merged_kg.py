from pandas.core.frame import DataFrame


class MergedKG:
    def __init__(self,
                 nodes: DataFrame,
                 edges: DataFrame,
                 duplicate_nodes: DataFrame,
                 dangling_edges: DataFrame
                 ):
        self.nodes = nodes
        self.edges = edges
        self.duplicate_nodes = duplicate_nodes
        self.dangling_edges = dangling_edges
