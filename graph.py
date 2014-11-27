
import graphlab as gl
import relaxmap

class GraphWrapper(object):

    def __init__(self, vertices, edges, parent=None):
        self.parent = parent
        self.g = gl.SGraph(vertices=vertices, edges=edges, 
            vid_field='v_id', src_field='src_id', dst_field='dst_id')

    def homes_for_the_homeless(self):
        """
        Randomly assign a community to nodes that need one
        """

    def do_pagerank(self):
        """"""

    def get_community_vertices(self):
        """
        groupby self.g['community_id']
        """
        self.community_vertices = gl.SFrame()

    def get_community_edges(self):
        """
        join g.edges w/ g.vertices to get SFrame like:
         - src_id, dst_id, src_community_id, dst_community_id

        groupby src_community_id, dst_community_id to get community_edges
        """
        self.community_edges = gl.SFrame()

    def get_community_gw(self):
        self.homes_for_the_homeless()
        return Graph(self.get_community_vertices(), 
            self.get_community_edges(), parent=self)

    def find_communities(self, partitions=None):
        if not partitions:
            communities, mdl = relaxmap.find_communities(self.g)
            #update self.g.vertices w/ community info
        else:
            mdl = 0
            for partition in partitions:
                communities, _mdl = relaxmap.find_communities(partition)
                mdl += _mdl
                #update self.g.vertices w/ community info

        if self.parent:
            partition_graphs = []
            for partition in communities:
                #pull subgraph out of self.parent.g
                partition_graphs.append(partition_graph)
            return self.parent.find_communities(partitions=partition_graphs)
        return mdl

    def save(self):
        """"""
        self.vertices