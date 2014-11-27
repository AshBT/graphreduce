import graphlab
from graph import Graph

vertices = get_vertices()
edges = get_edges()
g = Graph(vertices, edges)
g.home_for_the_homeless()

for i in range(2):
    description_length = base_g.partition_and_update_communities()
    print description_length

g.save()
