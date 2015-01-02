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

