import unittest, inspect, os
from graphreduce.graph_wrapper import GraphWrapper

vertex_path = 'http://static.smarttypes.org/static/graphreduce/test_data/vertex.csv.gz'
edge_path = 'http://static.smarttypes.org/static/graphreduce/test_data/edge.csv.gz'

if __name__ == '__main__':
    gw, mdls = GraphWrapper.reduce(vertex_path, edge_path)

