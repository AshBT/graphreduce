
def users_top_users(user_id, scores, feature_ids):
    scores = scores.groupby('user_id', 
        {'score':gl.aggregate.CONCAT('community_id', 'score')},
        {'num_features':gl.aggregate.COUNT('community_id')})
    scores = scores[scores['num_features'] > len(feature_ids) * .20]
    print scores.num_rows()
    scores = scores.join(gw.verticy_descriptions, {'user_id':'__id'})
    user_score = scores[scores['user_id'] == user_id][0]
    def distance(row):
        total_distance = 0
        for x in feature_ids:
            score1 = user_score['score'].get(x)
            score2 = row['score'].get(x)
            if score1 and score2:
                dis = abs(score1 - score2)
            elif score1 or score2:
                dis1 = abs(score1 or score2)
                dis2 = 1 - abs(score1 or score2)
                dis = max(dis1, dis2) * 2
            else:
                dis = 0
            total_distance+=dis
        return total_distance

    scores['distance'] = scores.apply(distance)

    scores['distance'] = (scores['distance'] - scores['distance'].min()) \
        / (scores['distance'].max() - scores['distance'].min())
    return scores.sort('distance')

feature_ids = list(rep_communities['community_id'][:n_features])
feature_ids += list(dem_communities['community_id'][:n_features])
feature_ids = list(set(feature_ids))

print 'DNC users'
dem_users = users_top_users(dem_id, user_community_scores, feature_ids)
for x in dem_users[:10]:
    print str(x['distance'])[:4], x['screen_name'], '-', x['description'][:75]
print ''

print 'RNC users'
rep_users = users_top_users(rep_id, user_community_scores, feature_ids)
for x in rep_users[:10]:
    print str(x['distance'])[:4], x['screen_name'], '-', x['description'][:75]
print ''

"""
Now lets look for accounts between the DNC and RNC, so called swing accounts.
"""

def users_in_between(distances):
    n_dimensions = len(distances)
    _distances = distances[0]
    for x in distances[1:]:
        _distances = _distances.append(x)
    distances = _distances
    distances = distances.groupby('user_id', {'distances':gl.aggregate.CONCAT('distance')})
    def distance(row):
        if len(row['distances']) != n_dimensions:
            return None
        _distances = [x+1 for x in row['distances']]
        _distances = gl.SArray(_distances)
        return _distances.mean() * _distances.std()
    distances['distance'] = distances.apply(distance)
    distances = distances.dropna().join(gw.verticy_descriptions, {'user_id':'__id'})
    return distances.sort('distance')

print "Swing accounts"
equidistant_users = users_in_between([dem_users, rep_users])
for x in equidistant_users[:20]:
    if abs(x['distances'][0] - x['distances'][1]) < .15 or True:
        print x['screen_name'], x['user_id'], '-', x['description'][:100]
        print '\t', x['distance'], x['distances']
        print ''