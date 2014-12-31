
"""
$ python run.py vertex_path edge_path output_dir
"""
import sys
from graphreduce.graph_wrapper import GraphWrapper

if __name__ == "__main__":

    vertex_path = sys.argv[1]
    edge_path = sys.argv[2]
    output_dir = sys.argv[3]
    gw, mdls = GraphWrapper.reduce(vertex_path, edge_path, output_dir)
