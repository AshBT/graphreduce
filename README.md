Graphreduce
==============

## large scale network segmentation

Graphreduce finds community structure in large internet sized networks.

## built on the backs of giants

The code here attempts to be pretty lightweight and straightforward, the heavy 
lifting is done by:

 - [graphlab](http://graphlab.com)

 - [the map equation](http://www.mapequation.org/code.html)

## a few details

Graphreduce was built to identify communities of like minded-people in large, 
internet-scale social networks such as twitter, facebook, and youtube. In practice, 
it can be used to detect community structure in any directed weighted network.

This library is the engine behind [smarttypes.org](http://www.smarttypes.org/), 
a free web service aimed at identifying niche communities, and highlighting what 
they talk about and find interesting over time.

We owe a lot to [graphlab](http://graphlab.com/learn/), a free data analysis 
framework aimed at enabling web scale machine learning and complex network analysis.

We're also greatly indebted to the work done by [Martin Rosvall](http://www.tp.umu.se/~rosvall/) 
and map equation team. The map equation is an information theory based objective 
function used to quantify community detection. 

Dare we not forget the father of information theory [Claude Shannon](http://en.wikipedia.org/wiki/Claude_Shannon).

