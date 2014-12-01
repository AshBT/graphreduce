import os, random
from datetime import datetime
import graphlab as gl

def find_communities(sgraph):
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    timestamp = timestamp + str(random.randint(1,1000))
    input_f = './relaxmap/input/%s.txt' % timestamp
    output_dir = './relaxmap/output/'
    vertices = sgraph.get_vertices()
    vertices.sort('__id')
    v_idx_map = dict([(x['__id'], i) for i, x in enumerate(vertices)])
    with open(input_f, 'w') as f:
        for row in sgraph.edges:
            f.write('%s %s %s \n' % (v_idx_map[row['__src_id']], 
                v_idx_map[row['__dst_id']], row['weight']))
    command = "./relaxmap/ompRelaxmap %(seed)s %(network_data)s %(threads)s %(attempts)s "+\
        "%(threshold)s %(vThresh)s %(maxIter)s %(outDir)s %(prior)s"
    params = {
        'seed':1,
        'network_data':input_f,
        'threads':4,
        'attempts':1,
        'threshold':1e-3,
        'vThresh':0.0,
        'maxIter':10,
        'outDir':output_dir,
        'prior':'prior'
    }
    command = command % params
    os.system(command)
    mdl = 0
    node_comm_map = {}
    with open(output_dir+'%s.tree' % timestamp, 'r') as f:
        for i, line in enumerate(f):
            if i == 0:
                mdl = float(line.split(' ')[3])
                continue
            comm_id = line.split(':')[0]
            node_id = line.split()[-1].strip('"')
            node_comm_map[node_id] = comm_id    
    comm_ids = []
    for i in range(vertices.num_rows()):
        comm_id = node_comm_map.get(str(i), random.choice(node_comm_map.values()))
        comm_ids.append(comm_id)
    vertices['community_id'] = gl.SArray(comm_ids)
    return vertices, mdl
