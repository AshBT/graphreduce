"""
# Explore your ego

What's possible now that the worlds interests and relationships are digitized and available for 
download?

In this notebook we'll explore the twitter ego network of presidential hopeful 
[Jeb Bush](https://twitter.com/JebBush). 

We'll discover like minded political and social interest communities.
Examine influence and similarity between these communities, and walk the political path 
from right to left, noting communities of interest between. In a follow-up post we'll
look at using these communities as latent features to predict content of interest. 

This notebook depends on:

 - [GraphLab](http://graphlab.com)
 - [The map equation](http://arxiv.org/abs/0906.1405)
 - [Relaxmap](http://uwescience.github.io/RelaxMap/)
 - [GraphReduce](https://github.com/timmytw/graphreduce)

This will get you everything you need:

```shell
$ git clone https://github.com/timmytw/graphreduce.git

$ cd graphreduce/; pip install -r requirements.txt
```
"""

import graphlab as gl
from graphreduce.graph_wrapper import GraphWrapper

"""
We'll start by downloading Jeb's preassembled ego net and looking for compression patterns 
(communities) within. This will be the most time consuming part 
of our exercise, it takes roughly 3 mins on my mechanical drive / 8gb ram / i7 laptop.
"""

v_path = 'http://static.smarttypes.org/static/graphreduce/test_data/JebBush.vertex.csv.gz'
e_path = 'http://static.smarttypes.org/static/graphreduce/test_data/JebBush.edge.csv.gz'
gw, mdls = GraphWrapper.reduce(v_path, e_path)

"""
OK, what did that do? 
A few things:
It found communities (compression patterns) in Jeb's ego network.
It identified relationships between communities, weighted links.
Using account descriptions, it tagged / labeled discovered communities,
allowing use to search and get an idea of the collective interests of the communities.

The topic of network based community detection is broad and deep, your author has spent 
a lot of time testing out various community detection algorithms and methodologies. 
The method here, [the map equation](http://www.mapequation.org/code.html), 
was developed by [Martin Rosvall](http://www.tp.umu.se/~rosvall/).
The method uses information theory to quantify compression of a random walk.
[Relaxmap](http://uwescience.github.io/RelaxMap/) is a parallel implementation of the map equation
objective.

Top communities
"""

def top_labels(labels_dict):
    labels = sorted(labels_dict.items(), key=lambda x: x[1], reverse=True)[:5]
    return [x[0] for x in labels]
communities = gw.g.get_vertices()
communities = communities[communities['member_count'] > 30]
communities['labels'] = communities['labels'].apply(top_labels)

user_community_scores = gw.child.user_community_scores()
def top_communities(user_id):
    user_scores = user_community_scores[user_community_scores['__id'] == user_id]
    user_scores = user_scores.pack_columns(column_prefix='score', dtype=dict, 
        new_column_name='score')
    user_scores = user_scores.stack('score', new_column_name=['community_id', 
        'score'])
    user_scores = communities.join(user_scores, {'__id':'community_id'})
    return user_scores.sort('score', ascending=False)

jeb_id = '113047940'
jeb_communities = top_communities(jeb_id)

"""
Reciprocation differences.

User interest > community interest

Community interest > user interest
"""

"""
Who should Jeb flatter?

Influential members of communities w/ uneven reciprocation.
"""

"""
Shortest path to get to these people.
"""



