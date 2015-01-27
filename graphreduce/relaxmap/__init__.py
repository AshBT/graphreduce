import os, random, inspect
from datetime import datetime
import graphlab as gl

"""
./ompRelaxmap 1 input/20141219204618213.txt 4 1 0.001 0 10 output/ prior
"""

_this_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
_cache_dir = os.environ.get('GRAPHREDUCE_RELAXMAP_FILE_LOCATIONS')
if not _cache_dir:
    _cache_dir = _this_dir

def find_communities(sgraph, threads=4):
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    timestamp = timestamp + str(random.randint(1,1000))
    input_f = _cache_dir + '/input/%s.txt' % timestamp
    if not os.path.exists(os.path.dirname(input_f)):
        os.makedirs(os.path.dirname(input_f))
    output_dir = _cache_dir + '/output/'
    if not os.path.exists(os.path.dirname(output_dir)):
        os.makedirs(os.path.dirname(output_dir))

    #input file
    vertices = sgraph.get_vertices()
    vertices.sort('__id')
    v_idx_map = dict([(x['__id'], i) for i, x in enumerate(vertices)])
    with open(input_f, 'w') as f:
        for row in sgraph.edges:
            f.write('%s %s %s \n' % (v_idx_map[row['__src_id']], 
                v_idx_map[row['__dst_id']], row['weight']))

    #run relaxmap
    command = "%(_this_dir)s/ompRelaxmap %(seed)s %(network_data)s %(threads)s %(attempts)s "+\
        "%(threshold)s %(vThresh)s %(maxIter)s %(outDir)s %(prior)s >/dev/null 2>&1"
    params = {
        '_this_dir':_this_dir,
        'seed':1,
        'network_data':input_f,
        'threads':threads,
        'attempts':1,
        'threshold':1e-4,
        'vThresh':0.0,
        'maxIter':15,
        'outDir':output_dir,
        'prior':'prior'
    }
    command = command % params
    os.system(command)

    #output file
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

    #vertices sframe
    vertices['community_id'] = gl.SArray(comm_ids)
    return vertices, mdl
