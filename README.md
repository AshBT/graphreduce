Graphreduce
==============

## Large scale network reduction

Graphreduce finds hierarchical community structure (compression patterns) 
in large complex networks.

The code attempts to be pretty lightweight and straightforward, the heavy 
lifting is done by:

 - [GraphLab](http://graphlab.com)
 - [The map equation](http://www.mapequation.org/code.html)
 - [Relaxmap](http://uwescience.github.io/RelaxMap/)

## Some details

Graphreduce was built to identify expert communities in large, 
internet-scale social networks such as twitter, facebook and youtube. 
It can be used to find hierarchical structure in any directed weighted 
graph or network.

We owe a lot to [GraphLab](http://graphlab.com/learn/), a free data analysis 
framework aimed at enabling web scale machine learning and network analysis.

We're also indebted to the work done by [Martin Rosvall](http://www.tp.umu.se/~rosvall/) 
and map equation team. The map equation is an information theory based objective 
function used to quantify network pattern detection. [Relaxmap](http://uwescience.github.io/RelaxMap/) 
is a parallel implementation of the map equation objective. Dare we not mention the father 
of information theory [Claude Shannon](http://en.wikipedia.org/wiki/Claude_Shannon).

## Using Graphreduce

Clone the git repro:

```
$ git clone git@github.com:timmytw/graphreduce.git
```

Install dependencies:

```
$ pip install -r graphreduce/requirements.txt
```

Make sure everything is working:

```
$ python graphreduce/test.py
```

Run on your own graph:

```
$ python graphreduce/run.py edge.csv [vertex.csv]
```

### Input

edge.csv has two required columns and one optional column. The first column 
is the id of the source vertex, the second column is the id of the destination vertex, 
the third optional column is the the edge weight.

edge.csv:
 - src_id
 - dest_id
 - [edge_weight]

vertex.csv is optional and used to seed the detection process w/ prior
information. This is useful in cases where your system has existing knowledge of the 
community structure, maybe from a previous graphreduce run or from some other 
information source. vertex.csv has two required columns.

vertex.csv:
 - vertex_id
 - community_id

### Output

graphreduce/run.py logs its progress to stout, and outputs four files to 
graphreduce/results/YYYYMMDDhhmmss:

graphreduce/results/YYYYMMDDhhmmss/vertex.csv
 - vertex_id
 - community_id

graphreduce/results/YYYYMMDDhhmmss/community.csv
 - community_id
 - partition_id

graphreduce/results/YYYYMMDDhhmmss/community_edge.csv
 - src_community_id
 - dest_community_id
 - edge_weight

graphreduce/results/YYYYMMDDhhmmss/process.log:
 - start_time
 - finish_time
 - number_of_vertices
 - number_of_edges
 - description_length
 - second_level_description_length
 - number_of_communities
 - number_of_partitions

description_lengh denotes how well our community detection process did.

## See it in action

Graphreduce is a key part of [SmartTypes.org](http://www.smarttypes.org/), 
a free web service identifying niche community experts, and highlighting 
what they talk about and find interesting over time.

