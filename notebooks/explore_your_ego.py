"""
# Explore your ego

In this notebook we'll analyze a 2 degree twitter ego network, that of your distinguished author.

We'll start by looking for compression patterns (communities) w/in the network, and using 
discovered patterns to build a new (smaller) network that approximates the first. 
We'll then look at how we can use this compressed network.

This notebook depends on:

 - [GraphLab](http://graphlab.com)
 - [The map equation](http://www.mapequation.org/code.html)
 - [Relaxmap](http://uwescience.github.io/RelaxMap/)
 - [GraphReduce](https://github.com/timmytw/graphreduce)
"""

import graphlab as gl
from graphreduce.graph_wrapper import GraphWrapper

"""
We'll start by pulling in and reducing our ego network, this will be the most time consuming part 
of our exercise, this takes roughly 9 mins on my mechanical drive / 8GB / i7 laptop.
"""

v_path = 'http://static.smarttypes.org/static/graphreduce/test_data/SmartTypes.vertex.csv.gz'
e_path = 'http://static.smarttypes.org/static/graphreduce/test_data/SmartTypes.edge.csv.gz'
gw, mdls, verticy_descriptions = GraphWrapper.reduce(v_path, e_path)

"""
OK, what can we do w/ this? We're going to:

 - Identify communities 
 - Identify first degree accounts related to this communities
 - Zoom in on a community and show it's most popular members
 - Show similar and dissimilar communities
 - Show communities on the path from one community to another

"""

