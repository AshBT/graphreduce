import graphlab as gl

def top_labels(gw, community_id, n_labels):
    labels = gw.g.vertices[gw.g.vertices['__id'] == community_id][0]['labels']
    return sorted(labels.items(), key=lambda x: x[1], reverse=True)[:n_labels]

def top_label(gw):
    def label(row):
        label = ''
        if not row['labels']:
            return label
        labels = sorted(row['labels'].items(), key=lambda x: x[1], reverse=True)[:3]
        label = '_'.join([x[0] for x in labels])
        label = '%s_%s' % (label, row['__id'])
        return label
    gw.g.vertices['label'] = gw.g.vertices.apply(label)

def top_subgraph(gw, n_vertices, n_edges):
    vertices = gw.g.vertices[gw.g.vertices['member_count'] > 200]
    vertices = vertices.topk('pr', n_vertices)
    ids = gl.SFrame(vertices['__id']).rename({'X1':'__id'})
    edges = gw.g.edges.join(ids, {'__src_id':'__id'}).join(ids, {'__dst_id':'__id'})
    edges = edges.topk('weight', n_edges)
    return gl.SGraph(vertices=vertices, edges=edges, 
        vid_field='__id', src_field='__src_id', dst_field='__dst_id')

def community_members(gw, community_id):
    return gw.child.g.vertices[gw.child.g.vertices['community_id'] == community_id]

def user_community_links(self):
    edges = self.g.edges.join(self.g.vertices, {'__dst_id':'__id'})
    edges.rename({'community_id':'dst_community_id'})
    grouped_edges = edges.groupby(['__src_id', 'dst_community_id'], 
        {'sum':gl.aggregate.SUM('weight')})

    member_counts = self.g.vertices.groupby('community_id', 
        {'member_count':gl.aggregate.COUNT('__id')})
    grouped_edges = grouped_edges.join(member_counts, {'dst_community_id':'community_id'})
    grouped_edges['weight'] = grouped_edges['sum'] / grouped_edges['member_count']

    grouped_edges_max = grouped_edges.groupby('__src_id', {'max':gl.aggregate.MAX('weight')})
    grouped_edges = grouped_edges.join(grouped_edges_max, '__src_id')
    grouped_edges['weight'] = grouped_edges['weight'] / grouped_edges['max']

    grouped_edges.remove_column('sum')
    grouped_edges.remove_column('max')
    grouped_edges.remove_column('member_count')
    return grouped_edges

def user_community_scores(self):
    user_community_links = self.user_community_links()
    community_user_links = self.community_user_links()
    scores = user_community_links.join(community_user_links, 
        {'__src_id':'__dst_id', 'dst_community_id':'src_community_id'})
    scores['score'] = scores['weight'] * scores['weight.1']
    scores.remove_columns(['weight', 'weight.1'])
    scores.rename({'__src_id':'__id', 'dst_community_id':'community_id'})
    scores = scores.groupby('__id', {'score':gl.aggregate.CONCAT('community_id', 'score')})
    scores = scores.unpack('score')
    return scores

