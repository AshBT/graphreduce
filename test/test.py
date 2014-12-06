
from graph_wrapper import GraphWrapper

import inspect, os
_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))


vertex_path = _dir + '/vertex.csv'
edge_path = _dir + '/edge.csv'

#if path doesn't exist get it from static.smarttypes.com

gw = GraphWrapper(GraphWrapper.load_vertices(vertex_path), 
    GraphWrapper.load_edges(edge_path))

hierarchy_levels = 2
for i in range(hierarchy_levels - 1):
    gw = gw.get_community_gw()

mdl1 = gw.find_communities()
mdl2 = gw.find_communities()
mdl3 = gw.find_communities()
print mdl1, mdl2, mdl3
gw.save('./output/vertex.csv', './output/edge.csv')