Graphreduce
==============

## Large scale network reduction

Graphreduce finds hierarchical community structure (compression patterns) 
in large complex networks.

The code here attempts to be pretty lightweight and straightforward, the heavy 
lifting is done by:

 - [GraphLab](http://graphlab.com)
 - [The map equation](http://www.mapequation.org/code.html)
 - [Relaxmap](http://uwescience.github.io/RelaxMap/)

## Some details

Graphreduce was built to squeeze the essence out of large complex networks. 
It can find hierarchical structure in any directed weighted net.

We owe a lot to [GraphLab](http://graphlab.com/learn/), a free data analysis 
framework aimed at enabling web scale machine learning and network analysis.

We're also indebted to the work done by [Martin Rosvall](http://www.tp.umu.se/~rosvall/) 
and map equation team. The map equation is an information theory centered objective 
function used to quantify pattern detection on networks. [Relaxmap](http://uwescience.github.io/RelaxMap/) 
is a parallel implementation of the map equation objective. Dare we not mention the father 
of information theory [Claude Shannon](http://en.wikipedia.org/wiki/Claude_Shannon).

## Try it

```
$ git clone https://github.com/timmytw/graphreduce.git
```

```
$ cd graphreduce/; pip install -r requirements.txt
```

```
$ python graphreduce/test/test.py
```

## Run it on your own network

```
$ python graphreduce/reduce.py vertex_path edge_path output_dir
```

## See it in action

Graphreduce is a key part of [SmartTypes.org](http://www.smarttypes.org/), 
a free web service aimed at social exploration.

Get in touch @ hello@smarttypes.org
