
import graphlab as gl

class Graph(object):

    def __init__(self, vertices, edges, parent=None):
        self.parent = parent
        self.vertices = vertices
        self.edges = edges
        self.sgraph = gl.SGraph(vertices=self.vertices, edges=self.edges, 
            vid_field='v_id', src_field='src_id', dst_field='dst_id')

    def homes_for_the_homeless(self):
        """
        Assign a community to nodes that need one
        """

    def pagerank_em(self):
        """"""

    def set_community_vertices(self):
        """
        groupby self.sgraph['community_id']
        """
        self.community_vertices = gl.SFrame()

    def set_community_edges(self):
        """
        join g.edges w/ g.vertices to get SFrame like:
         - src_id, dst_id, src_community_id, dst_community_id

        groupby src_community_id, dst_community_id to get community_edges
        """
        self.community_edges = gl.SFrame()

    def get_community_g(self):
        return Graph(self.community_vertices, self.community_edges, parent=self)

    def find_communities(self):
        "return list of Graphs"
        if self.parent:
            #pull subgraph out of self.parent.g
            #partition_community_ids = self.parent.g['v_id']
            #partition_g = self.sgraph[self.sgraph['community_id'] in _partition_community_ids]

    def partition_and_update_communities(self):
        self.homes_for_the_homeless()
        self.set_community_vertices()
        self.set_community_edges()
        community_g = self.get_community_g()
        total_mdl = 0
        for partition_g, partition_mdl in community_g.find_communities():
            for partition_community_g, partition_community_mdl in partition_g.find_communities():
                total_mdl += partition_community_mdl
                "update self.sgraph.vertices, set community_id"
                "update self.sgraph.community_vertices, set new community_ids and partition_ids"
        return total_mdl

    def save(self):
        """"""