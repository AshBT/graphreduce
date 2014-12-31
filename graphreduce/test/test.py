import unittest, inspect, os
from graphreduce.graph_wrapper import GraphWrapper

_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

vertex_path = _dir + '/vertex.csv.gz'
edge_path = _dir + '/edge.csv.gz'
if not os.path.exists(vertex_path):
    vertex_path = 'http://static.smarttypes.org/static/graphreduce/test_data/vertex.csv.gz'
    edge_path = 'http://static.smarttypes.org/static/graphreduce/test_data/edge.csv.gz'

if __name__ == '__main__':
    gw, mdls = GraphWrapper.reduce(vertex_path, edge_path)

