"""
# Twitter Politics

In this notebook we'll explore the twitter networks of both 
sides of US political aisle [TheDemocrats](https://twitter.com/TheDemocrats) 
and the [GOP](https://twitter.com/GOP).

We'll identify like minded political and social interest communities related 
to both sides. We'll use these discovered communities as latent features 
to measure social distance / similarity. Finally we'll look for twitter 
accounts equally influential to both sides, so called swing accounts.

This notebook depends on:

 - [GraphLab create](http://dato.com/products/create/index.html)
 - [The map equation](http://arxiv.org/abs/0906.1405)
 - [Relaxmap](http://uwescience.github.io/RelaxMap/)
 - [GraphReduce](https://github.com/timmytw/graphreduce)

This will get you everything you need:

```shell
$ git clone https://github.com/timmytw/graphreduce.git

$ cd graphreduce/; pip install -r requirements.txt
```
"""

import os, math, inspect
from operator import mul
import graphlab as gl
from graphreduce.graph_wrapper import GraphWrapper

"""
We'll start by downloading the combined preassembled 2 degree ego network of 
[TheDemocrats](https://twitter.com/TheDemocrats) and the 
[GOP's](https://twitter.com/GOP) twitter accounts.
Once we have the network of interest 
we'll dig through it looking for compression patterns (communities).
This will be the 
most time consuming part of our exercise, it takes roughly 3 mins 
on my mechanical drive / 8gb ram / i7 laptop.
"""

this_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
cache_dir = this_dir+'/.twitter_politics/'
if os.path.exists(cache_dir+'parent'):
    gw = GraphWrapper.from_previous(cache_dir)
else:
    v_path = 'http://static.smarttypes.org/static/graphreduce/test_data/TheDemocrats_GOP.vertex.csv.gz'
    e_path = 'http://static.smarttypes.org/static/graphreduce/test_data/TheDemocrats_GOP.edge.csv.gz'
    gw, mdls = GraphWrapper.reduce(v_path, e_path)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    gw.save(cache_dir)

"""
OK, what did this do?
As mentioned above it discovered compression patterns (pockets of link density)
within our network, and identified relationships between these communities.
Using account descriptions it tagged / labeled the communities,
allowing use to get a quick idea of the collective interests.

The topic of network based community detection is broad and deep.
The method here, 
[the map equation](http://arxiv.org/abs/0906.1405),
uses information theory to quantify compression of a random walk.
[Relaxmap](http://uwescience.github.io/RelaxMap/), the library we use, 
is a parallel implementation of the map equation objective.

Let's take a look at some of the discovered communities.
"""

execfile('scratch.py')
































# """
# Our community detection algo discovered a Bush family community.
# Not surprisingly this is the one Jeb is closest to.
# Let's look at some of the group's more influential members.
# """
# def users_between_communities(community_ids, scores):
#     num_comms = len(community_ids)
#     community_ids = gl.SFrame({'__id': community_ids})
#     scores = scores.join(community_ids, {'community_id':'__id'})
#     scores = scores.groupby('__id', {'score':gl.aggregate.CONCAT('score'), 
#         'user_interest':gl.aggregate.CONCAT('user_interest'), 
#         'community_interest':gl.aggregate.CONCAT('community_interest')})
#     def _score(row):
#         if len(row['score']) != num_comms:
#             return None
#         _diff = max(row['score']) - min(row['score'])
#         return reduce(mul, row['score'], 1) / _diff
#     scores['score'] = scores.apply(_score)
#     scores = scores.dropna().join(gw.verticy_descriptions, '__id')
#     return scores.sort('score', ascending=False)

# dem_comm_ids = [x['__id'] for x in dem_communities[:1]]
# rep_comm_ids = [x['__id'] for x in rep_communities[:1]]
# comm_ids = dem_comm_ids + rep_comm_ids
# for x in users_between_communities(comm_ids, user_community_scores)[:10]:
#     print x['score'], x['screen_name'], x['description'] #x['user_interest'], x['community_interest']

# #x['description'],

# # def show_community_members(community_id):
# #     comm_members = gw.child.g.vertices[gw.child.g.vertices['community_id'] == community_id]
# #     comm_members = comm_members.join(gw.verticy_descriptions, '__id')
# #     for x in comm_members:
# #         print x['__id'], x['description']
# # show_community_members(jeb_reciprocal_communities[0]['__id'])






