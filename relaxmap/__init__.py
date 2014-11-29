import os, random
from datetime import datetime
import graphlab as gl

def find_communities(sgraph):
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    timestamp = timestamp + str(random.randint(1,1000))
    input_f = './relaxmap/input/%s.txt' % timestamp
    output_dir = './relaxmap/output/'
    with open(input_f, 'w') as f:
        for row in sgraph.edges:
            f.write('%(__src_id)s %(__dst_id)s %(weight)s \n' % row)
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
            node_id = line.split(' ')[-1]



            comm_ids.append(line.split(':')[0])
    vertices = sgraph.get_vertices()
    vertices.sort('__id')
    print vertices.num_rows(), len(comm_ids)
    vertices['community_id'] = gl.SArray(comm_ids)
    return vertices, mdl
