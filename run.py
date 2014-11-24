from datetime import datetime
import graph

g = graph.Graph()
g.add_edges_from_file('edge.csv')
g.add_vertices_from_file('vertex.csv')
g.assign_random_home_to_the_homeless()

for i in range(2):
    partition_g = g.create_network_of_communities()
    partition_g.find_communities()
    for partition in partition_g.communities:
        partition.find_communities()

partition_g = g.create_network_of_communities()
partition_g.find_communities()

g.save_to_disk()