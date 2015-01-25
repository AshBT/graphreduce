"""
# Using twitter to assess political strategy and position

In this notebook we'll explore the twitter networks of both 
sides of US political aisle: [TheDemocrats](https://twitter.com/TheDemocrats) 
and the [GOP](https://twitter.com/GOP).

We'll identify like minded political and social interest communities, and use these 
communities as latent features (landmarks) to quantify relatedness.

What can this data really tell us? Our world is messy and 
complicated. Online social networks like twitter and facebook are 
representative of our world in the same way much of our fiction, media, and 
dare I say non-fiction are representative, they're biased distortions
of a beautifully complex reality. With this in mind, the internet marks a huge 
leap in the availability of social (albeit distorted) information. 
Your author remains curious and optimistic about what this distorted lens can teach us 
about ourselves and the chaotic world we inhabit.

This notebook depends on:

 - [GraphLab create](http://dato.com/products/create/index.html)
 - [The map equation](http://arxiv.org/abs/0906.1405)
 - [Relaxmap](http://uwescience.github.io/RelaxMap/)
 - [GraphReduce](https://github.com/timmytw/graphreduce)

This gets you everything you need:

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
We'll start by downloading the combined preassembled 2 degree ego networks of 
[TheDemocrats](https://twitter.com/TheDemocrats) and the 
[GOP](https://twitter.com/GOP).
Once we have the combined network we'll mine it for compression patterns (communities).
This will be the 
most time consuming part of our exercise, it takes roughly 4 mins 
on my magnetic drive / 8gb ram / i7 laptop.
"""

this_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
cache_dir = this_dir+'/.twitter_politics/'
if os.path.exists(cache_dir+'parent'):
    gw = GraphWrapper.from_previous_reduction(cache_dir)
else:
    v_path = 'http://static.smarttypes.org/static/graphreduce/test_data/TheDemocrats_GOP.vertex.csv.gz'
    e_path = 'http://static.smarttypes.org/static/graphreduce/test_data/TheDemocrats_GOP.edge.csv.gz'
    gw, mdls = GraphWrapper.reduce(v_path, e_path)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    gw.save(cache_dir)

"""
OK, what happened? We discovered compression patterns in our network, 
pockets of dense vertex interlinking.
In practice, as we'll see, twitter accounts with similar interests / 
motivations tend to follow each other.

The topic of network based community detection is broad and deep. It has 
many applications outside social network analysis, the network abstraction is
used to model many of the world's most complex problems.
As you might imagine network compression / pattern recognition has a fertile litter of applications.
The method here, 
[the map equation](http://arxiv.org/abs/0906.1405),
uses information theory to quantify compression of a random walk.
[Relaxmap](http://uwescience.github.io/RelaxMap/), the library used here, 
is a parallel implementation of the map equation objective.

Using account descriptions, the method also tagged / labeled the communities. 
These tags allow use to search and get a quick idea of the collective interests 
of a community. The tagging procedure uses [tf-idf](http://en.wikipedia.org/wiki/Tf-idf)
Let's take a look at the most popular communities, order by pagerank.
"""

min_members = 25
communities = gw.g.get_vertices()
communities = communities[communities['member_count'] >= min_members]
communities['pr'] = communities['pr'] / communities['pr'].max()
print 'Popular communities'
for x in communities.sort('pr', ascending=False)[:10]:
    print str(x['pr'])[:4], x['member_count'], x['top_labels']
print ''

"""
Let's look @ communities close to the respective parties.
"""

def reciprocal_interest(scores):
    def _score(row):
        return row['user_interest'] * row['community_interest']
    return scores.apply(_score)

user_community_scores = gw.child.user_community_scores(reciprocal_interest, min_members)

def users_top_communities(user_id, scores):
    user_scores = scores[scores['user_id'] == user_id]
    user_scores = user_scores.join(communities, {'community_id':'__id'})
    user_scores.remove_column('community_id.1')
    return user_scores.sort('score', ascending=False)

print 'DNC communities'
dem_id = '14377605'
dem_communities = users_top_communities(dem_id, user_community_scores)
for x in dem_communities[:10]:
    print str(x['score'])[:4], x['member_count'], x['top_labels']
print ''

print 'RNC communities'
rep_id = '11134252'
rep_communities = users_top_communities(rep_id, user_community_scores)
for x in rep_communities[:10]:
    print str(x['score'])[:4], x['member_count'], x['top_labels']
print ''

"""
The 'score' here is the product of user_interest and community_interest.
Twitter is a directed network, our objective function rewards relationships 
where an account follows many people in a community and many people in the 
community follow the account, a reciprocal_interest function.

What can we glean from this? I'm not really sure. But there are a few things worth 
mentioning.

The DNC is aligned heavily with volunteers, colleges, and the news media.
And then, to a less extent, supportive and swing states.

The RNC is aligned primarily with conservatives, the media (less than 
the DNC), and with congress accounts. 
After this a mix of support and swing states.

Let's look @ communities of interest to both sides.
"""

#raise Exception('communities of interest to both sides')

"""
Influential members in these communities.
"""

#raise Exception('Influential community members')

"""
We'll use the communities closest to each party as features (landmarks) to gauge distance 
between the DNC / RNC and all other accounts.
"""

def users_top_users(user_id, scores, feature_ids):
    assert scores['score'].min() >= 0
    scores = scores.groupby('user_id', 
        {'score':gl.aggregate.CONCAT('community_id', 'score')},
        {'num_features':gl.aggregate.COUNT('community_id')})
    scores = scores[scores['num_features'] > len(feature_ids) * .20]
    user_score = scores[scores['user_id'] == user_id][0]
    def distance(row):
        total_distance = 0
        for x in feature_ids:
            score1 = user_score['score'].get(x)
            score2 = row['score'].get(x)
            if score1 and score2:
                dis = abs(score1 - score2)
            elif score1 or score2:
                dis = (score1 or score2) * 2
            else:
                dis = 0
            total_distance+=dis
        return total_distance
    scores['distance'] = scores.apply(distance)
    scores = scores.join(gw.verticy_descriptions, {'user_id':'__id'})
    scores['distance'] = (scores['distance'] - scores['distance'].mean()) \
        / (scores['distance'].std())
    return scores.sort('distance')

feature_ids = list(rep_communities['community_id'][:5])
feature_ids += list(dem_communities['community_id'][:5])
feature_ids = list(set(feature_ids))

print 'Accounts similar to the DNC:'
dem_users = users_top_users(dem_id, user_community_scores, feature_ids)
for x in dem_users[:10]:
    print str(x['distance'])[:4], x['screen_name'], '-', x['description'][:75]
print ''

print 'Accounts similar to the RNC:'
rep_users = users_top_users(rep_id, user_community_scores, feature_ids)
for x in rep_users[:10]:
    print str(x['distance'])[:4], x['screen_name'], '-', x['description'][:75]
print ''

"""
Now lets look for accounts of interest to the DNC and RNC.
"""

def users_in_between(distances):
    n_dimensions = len(distances)
    _distances = distances[0]
    for x in distances[1:]:
        _distances = _distances.append(x)
    distances = _distances
    #assert distances['distance'].min() >= 0
    distances = distances.groupby('user_id', {'distances':gl.aggregate.CONCAT('distance')})
    def between(row):
        if len(row['distances']) != n_dimensions:
            return None
        x = gl.SArray(row['distances'])
        if x.std() > .15:
            return None
        return x.mean() + x.std()
    distances['distance'] = distances.apply(between)
    distances = distances.dropna().join(gw.verticy_descriptions, {'user_id':'__id'})
    return distances.sort('distance')

print "Of interest to the DNC and RNC"
equidistant_users = users_in_between([dem_users, rep_users])
for x in equidistant_users[:10]:
    print x['screen_name'], '-', x['description'][:100]
    #print '\t', x['distance'], x['distances']

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






