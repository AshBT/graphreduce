
"""
$ python run.py vertex_path edge_path output_dir
"""
import sys
from graphreduce.graph_wrapper import GraphWrapper

class ProcessWrapper(object):

    def __init__(self, vertex_path, edge_path, output_dir, do_pagerank=False, do_tagging=False):
        self.vertex_path = vertex_path
        self.edge_path = edge_path
        self.output_dir = output_dir
        self.do_pagerank = do_pagerank
        self.do_tagging = do_tagging

    def run(self):
        gw = GraphWrapper(GraphWrapper.load_vertices(self.vertex_path), 
            GraphWrapper.load_edges(self.edge_path))
        hierarchy_levels = 3
        for i in range(hierarchy_levels - 1):
            gw = gw.get_community_gw()
        mdl1 = gw.find_communities()
        mdl2 = gw.find_communities()
        mdl3 = gw.find_communities()
        gw.save(output_dir+'community.csv', output_dir+'community_net.csv')
        print mdl1, mdl2, mdl3
        assert mdl1 > mdl2
        assert mdl2 > mdl3

if __name__ == "__main__":

    vertex_path = sys.argv[1]
    edge_path = sys.argv[2]
    output_dir = sys.argv[3]
    pw = ProcessWrapper(vertex_path, edge_path, output_dir)
    pw.run()
