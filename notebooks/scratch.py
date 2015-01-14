
communities = gw.g.get_vertices()
communities = communities[communities['member_count'] > 99]
print 'Popular communities'
for x in communities.sort('pr', ascending=False)[:10]:
    print x['pr'], x['member_count'], x['top_labels']
print ''

"""
Now let's look @ a few communities close to the respective parties.
"""
def reciprocal_interest(scores):
    def _score(row):
        return row['user_interest'] * row['community_interest'] * math.log(float(row['member_count']))
    return scores.apply(_score)
user_community_scores = gw.child.user_community_scores(reciprocal_interest)

def users_top_communities(user_id, scores):
    user_scores = scores[scores['__id'] == user_id]
    user_scores = communities.join(user_scores, {'__id':'community_id'})
    return user_scores.sort('score', ascending=False)

dem_id = '14377605'
rep_id = '11134252'

print 'DNC communities'
dem_communities = users_top_communities(dem_id, user_community_scores)
for x in dem_communities[:10]:
    print x['score'], x['member_count'], x['top_labels']
print ''

print 'RNC communities'
rep_communities = users_top_communities(rep_id, user_community_scores)
for x in rep_communities[:10]:
    print x['score'], x['member_count'], x['top_labels']
print ''

