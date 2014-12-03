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

## See it in action

Graphreduce is a key part of [SmartTypes.org](http://www.smarttypes.org/), 
a free web service identifying niche community experts, and highlighting 
what they talk about and find interesting over time.
