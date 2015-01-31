
# coding: utf-8

## Using twitter to assess political strategy and position

# In this notebook we'll explore the networks of both 
# sides of US political aisle: [TheDemocrats](https://twitter.com/TheDemocrats) 
# and the [GOP](https://twitter.com/GOP).
# 
# We'll identify like minded political and social interest communities, and use these 
# communities to measure social space.
# 
# ### What can this data really tell us?
# 
# > The Quality which creates the world emerges as a relationship between man and his experience. He is a participant in the creation of all things. The measure of all things. -- Robert Pirsig
# 
# Our socio political / economic world is messy and complicated. 
# Online social networks give us a small peak into this chaotic abyss.
# 
# Your author remains curious and optimistic about what this distorted lens can teach us.
# 
# This notebook depends on:
# 
#  - [GraphLab create](http://dato.com/products/create/index.html)
#  - [The map equation](http://arxiv.org/abs/0906.1405)
#  - [Relaxmap](http://uwescience.github.io/RelaxMap/)
#  - [GraphReduce](https://github.com/timmytw/graphreduce)
# 
# This will get you everything you need:
# 
# ```
#  $ git clone https://github.com/timmytw/graphreduce.git
# 
#  $ cd graphreduce/; pip install -r requirements.txt
# ```

# In[34]:

import os, math, inspect
from IPython.core.display import HTML
from operator import mul
import graphlab as gl
from graphreduce.graph_wrapper import GraphWrapper


# ## Downloading and compressing our network
# 
# First we'll download the preassembled 2 degree ego networks of the DNC and RNC,
# then we'll mine the 37K vertex / 9M edge network for compression patterns.
# 
# This will be the most time consuming part of our exercise, it takes roughly 5.5 mins 
# on my magnetic drive / 8gb ram / i7 laptop.
# 

# In[40]:

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


# ## Network community detection
# 
# The topic of community detection is broad and deep.
# The method here, 
# [the map equation](http://arxiv.org/abs/0906.1405),
# uses information theory to quantify the compression of a random walk on our network.
# [Relaxmap](http://uwescience.github.io/RelaxMap/) 
# is a parallel implementation of the map equation objective.
# 
# The method also tags communities, giving us a quick idea of 
# the collective interests of a community.
# 
# Let's take a look at the most popular communities, order by pagerank:

# In[36]:

min_members = 25
communities = gw.g.get_vertices()
communities = communities[communities['member_count'] >= min_members]
communities['pr'] = communities['pr'] / communities['pr'].max()
print '\nPopular communities:'
for x in communities.sort('pr', ascending=False)[:10]:
    print ' - ', str(x['pr'])[:4], x['member_count'], x['top_labels']


# ##Left and right communities
# 
# Let's look at communities close to the respective parties:

# In[37]:

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

print '\nDNC communities:'
dem_id = '14377605'
dem_communities = users_top_communities(dem_id, user_community_scores)
for x in dem_communities[:10]:
    print ' - ', str(x['score'])[:4], x['member_count'], x['top_labels']
print ''

print 'RNC communities:'
rep_id = '11134252'
rep_communities = users_top_communities(rep_id, user_community_scores)
for x in rep_communities[:10]:
    print ' - ', str(x['score'])[:4], x['member_count'], x['top_labels']


# ## What can we glean from this?
# 
# The 'score' here is the product of user_interest and community_interest.
# Twitter is a directed network, our objective function rewards relationships 
# where an account follows many people in a community and many people in the 
# community follow the account, a reciprocal_interest function.
# 
# What can we glean from this? I'm not really sure. But there are a few things worth 
# mentioning.
# 
# The DNC is aligned heavily with volunteers, colleges, and the news media.
# And then, to a less extent, supportive and swing states. I was surprised to see texas, 
# [Can Democrats Turn Texas and Arizona Blue by 2016?](http://fivethirtyeight.blogs.nytimes.com/2013/03/01/can-democrats-turn-texas-and-arizona-blue-by-2016/)
# 
# The RNC is aligned primarily with conservatives, the media, and congressional representation, 
# then a mix of it's own support and swing states.
# 
# We'll use the communities closest to each party as features (landmarks) 
# to measure similarity.
# Let's look at accounts inline with the respective parties:

# In[38]:

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
    scores['distance'] = (scores['distance'] - scores['distance'].mean())         / (scores['distance'].std())
    return scores.sort('distance')

feature_ids = list(rep_communities['community_id'][:5])
feature_ids += list(dem_communities['community_id'][:5])
feature_ids = list(set(feature_ids))

print '\nAccounts similar to the DNC:'
dem_users = users_top_users(dem_id, user_community_scores, feature_ids)
for x in dem_users[:10]:
    print ' - ', str(x['distance'])[:4], x['screen_name'], '-', x['description'][:75]
print ''

print 'Accounts similar to the RNC:'
rep_users = users_top_users(rep_id, user_community_scores, feature_ids)
for x in rep_users[:10]:
    print ' - ', str(x['distance'])[:4], x['screen_name'], '-', x['description'][:75]
print ''


# ## Accounts of interest to both sides
# 
# Now lets look for accounts of interest to both the DNC and RNC, so called swing accounts:

# In[39]:

def users_in_between(distances):
    n_dimensions = len(distances)
    _distances = distances[0]
    for x in distances[1:]:
        _distances = _distances.append(x)
    distances = _distances
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

print "\nOf interest to the DNC and RNC:\n"
equidistant_users = users_in_between([dem_users, rep_users])
for x in equidistant_users[:10]:
    print ' - ', x['screen_name'], '-', x['description'][:100]


# ## Supervised learning
# 
# The focus of this notebook has been unsupervised learning, 
# in a future notebook we'll look at using labeled data and network compression 
# patterns to predict vertex actions.
