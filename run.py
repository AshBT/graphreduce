import graphlab
from graph import Graph
import sys

vertices = graphlab.get_vertices(sys.args[1])
edges = graphlab.get_edges(sys.args[2])
gw = GraphWrapper(vertices, edges)

progress_threshold = 1000 #this will take some tweaking
progress = progress_threshold + 1
last_mdl = 1e20
while progress > progress_threshold:
    community_gw = gw.get_community_gw()
    mdl = community_gw.find_communities()
    print mdl
    progress = last_mdl - mdl
    last_mdl = mdl

g.save()
