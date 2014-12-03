
from graph_wrapper import GraphWrapper

edge_path = 'test_data/edge.csv'
vertex_path = 'test_data/vertex.csv'

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