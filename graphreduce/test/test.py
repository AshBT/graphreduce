import unittest, inspect, os
from graphreduce.graph_wrapper import GraphWrapper

_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

vertex_path = _dir + '/vertex.csv.gz'
edge_path = _dir + '/edge.csv.gz'
if not os.path.exists(vertex_path):
    vertex_path = 'http://static.smarttypes.org/static/graphreduce/test_data/vertex.csv.gz'
    edge_path = 'http://static.smarttypes.org/static/graphreduce/test_data/edge.csv.gz'

class TestSomeThings(unittest.TestCase):

    def test_two_level_reduction(self):
        gw = GraphWrapper(GraphWrapper.load_vertices(vertex_path), 
            GraphWrapper.load_edges(edge_path))

        hierarchy_levels = 3
        for i in range(hierarchy_levels - 1):
            gw = gw.get_community_gw()

        mdl1 = gw.find_communities()
        mdl2 = gw.find_communities()
        mdl3 = gw.find_communities()
        print mdl1, mdl2, mdl3
        assert mdl1 > mdl2
        assert mdl2 > mdl3

if __name__ == '__main__':
    unittest.main()
