
import graphlab as gl

class Graph(object):

    def __init__(self, vertices, edges):
        self.vertices = vertices
        self.edges = edges
        self.g = gl.SGraph(vertices=self.vertices, edges=self.edges, 
            vid_field='v_id', src_field='src_id', dst_field='dst_id')
        self.community_vertices = None
        self.community_edges = None
        self.community_g = None

    def home_for_the_homeless(self):
        """
        Assign a random community to the nodes w/out one
        """

    def set_community_vertices(self):
        """
        use self.g to set
        """
        self.community_vertices = gl.SFrame()

    def set_community_edges(self):
        """
        join g.edges w/ g.vertices to get SFrame like:
         - src_id, dst_id, src_community_id, dst_community_id

        groupby src_community_id, dst_community_id to get community_edges
        """
        self.community_edges = gl.SFrame()

    def set_community_g(self):
        self.community_g = Graph(self.community_vertices, self.community_edges)

    def find_communities(self):
        "return list of Graphs"

    def partition_and_update_communities(self):
        self.set_community_vertices()
        self.set_community_edges()
        self.set_community_g()

        total_mdl = 0
        for partition_g, partition_mdl in self.community_g.find_communities():
            #pull subgraph out of self.g
            partition_community_ids = partition_g.g['v_id']
            partition_g = self.g[self.g['community_id'] in _partition_community_ids]
            for community, community_mdl in partition_g.find_communities():
                total_mdl += _community_mdl
                "update self.g.vertices, set community_id"
                "update self.g.community_vertices, set new community_ids and partition_ids"
        return total_mdl