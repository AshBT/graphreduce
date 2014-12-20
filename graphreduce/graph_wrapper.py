
import random
import graphlab as gl
from graphreduce import relaxmap
from datetime import datetime

class GraphWrapper(object):

    def __init__(self, vertices, edges, child=None):
        #todo: make vertices optional
        self.parent = None
        self.child = child
        self.g = gl.SGraph(vertices=vertices, edges=edges, 
            vid_field='__id', src_field='__src_id', dst_field='__dst_id')

    AVG_COMM_SIZE = 400

    def homes_for_the_homeless(self):
        unique = [x for x in list(self.g.vertices['community_id'].unique()) if x]
        if not unique:
            unique = ['1']
        target_num_comms = int(self.g.vertices.num_rows() / self.AVG_COMM_SIZE)
        distance = target_num_comms - len(unique)
        if distance > 0:
            #assumes communities are 'int'able
            offset = int(max(unique)) + 1
            unique += [str(x+offset) for x in range(int(distance))]
        print ' - Num homeless: %s' % self.g.vertices['community_id'].num_missing()
        self.g.vertices['community_id'] = self.g.vertices['community_id'].apply(lambda x: 
            random.choice(unique) if not x else x, skip_undefined=False)

    def get_community_vertices(self):
        unique = [x for x in list(self.g.vertices['community_id'].unique()) if x]
        num_comms = len(unique) / self.AVG_COMM_SIZE if len(unique) > self.AVG_COMM_SIZE else 1
        num_comms = int(num_comms)
        comms = [str(x) for x in range(num_comms)]
        comms = [random.choice(comms) for x in unique]
        #community_id needed for multilevel hierarchies > than 2 levels
        return gl.SFrame({'__id': unique, 'community_id':comms})

    def get_community_edges(self):
        edges = self.g.edges.join(self.g.vertices, {'__dst_id':'__id'})
        edges.rename({'community_id':'dst_community_id'})
        edges = edges.join(self.g.vertices, {'__src_id':'__id'})
        edges.rename({'community_id':'src_community_id'})
        grouped_edges = edges.groupby(['src_community_id', 'dst_community_id'], {'sum':gl.aggregate.SUM('weight')})
        grouped_edges.rename({'src_community_id':'__src_id', 'dst_community_id':'__dst_id'})

        member_counts = self.g.vertices.groupby('community_id', {'member_count':gl.aggregate.COUNT('__id')})
        grouped_edges = grouped_edges.join(member_counts, {'__dst_id':'community_id'})
        grouped_edges['weight'] = grouped_edges['sum'] / grouped_edges['member_count']

        grouped_edges_max = grouped_edges.groupby('__src_id', {'max':gl.aggregate.MAX('weight')})
        grouped_edges = grouped_edges.join(grouped_edges_max, '__src_id')
        grouped_edges['weight'] = grouped_edges['weight'] / grouped_edges['max']

        #trim the tail
        #grouped_edges = grouped_edges[grouped_edges['weight'] > 0.001]

        grouped_edges.remove_column('sum')
        grouped_edges.remove_column('max')
        grouped_edges.remove_column('member_count')
        return grouped_edges

    def get_community_gw(self):
        if self.child:
            raise Exception('Graphreduce only supports two level hierarchies at this time.')
        self.homes_for_the_homeless()
        gw = GraphWrapper(self.get_community_vertices(), self.get_community_edges(), child=self)
        self.parent = gw
        return gw

    def find_communities(self, partitions=None):
        if not partitions:
            partitions = [self.g]

        #assumes only 2 levels for now
        level = 'Top' if self.child else 'Bottom'
        n_partitions = len(partitions)
        print '\n---------------------------------------------'
        print '%s level detection, %s partition(s)' % (level, n_partitions)
        print '---------------------------------------------'

        mdl = 0
        vertices = None
        for i, partition in enumerate(partitions):
            print ' - partition %s of %s, v_count: %s' % (i+1, n_partitions, 
                partition.vertices.num_rows())

            #relaxmap seems to fail randomly w/ a segmentation error
            #running w/ 1 thread fixes the problem
            try:
                _vertices, _mdl = relaxmap.find_communities(partition, 4)
            except Exception:
                _vertices, _mdl = relaxmap.find_communities(partition, 1)
            _n_communities = len(_vertices['community_id'].unique())
            print '   + found %s communities, mdl: %s\n' % (_n_communities, _mdl)

            mdl += _mdl
            if not vertices:
                vertices = _vertices
            else:
                _offset = len(vertices['community_id'].unique())
                _vertices['community_id'] = _vertices['community_id'].apply(
                    lambda x: str(int(x) + _offset))
                vertices = vertices.append(_vertices)
                #assert len(vertices['community_id'].unique()) - len(_vertices['community_id'].unique()) == _offset

        #update w/ community info
        #vertices = self.g.vertices.join(vertices, '__id', how='left')
        #vertices.remove_column('community_id')
        #vertices.rename({'community_id.1':'community_id'})
        self.g = gl.SGraph(vertices=vertices, edges=self.g.get_edges(), 
            vid_field='__id', src_field='__src_id', dst_field='__dst_id')
        self.homes_for_the_homeless()

        #use discovered communities to pull sgraph partitions out of child
        if self.child:
            partitions = []
            _community_ids = []
            grouped_vertices = self.g.get_vertices().groupby('community_id', {'__ids': gl.aggregate.CONCAT('__id')})
            num_grouped_vertices = grouped_vertices.num_rows()
            print ' - found %s communities' % num_grouped_vertices
            grouped_vertices = list(grouped_vertices)
            random.shuffle(grouped_vertices)
            for i, row in enumerate(grouped_vertices):
                _community_ids.extend(row['__ids'])
                if len(_community_ids) < 100 and (i + 1) < num_grouped_vertices:
                    continue
                _vertices = gl.SFrame({'__id':_community_ids})
                _community_ids = []
                _num_vertices = _vertices.num_rows()
                print '   + mk partition vertex count: %s' % _num_vertices
                p_vertices = self.child.g.vertices.join(_vertices, {'community_id':'__id'})
                if not p_vertices.num_rows():
                    continue
                p_edges = self.child.g.edges.join(p_vertices, {'__src_id':'__id'})
                if not p_edges.num_rows():
                    continue
                p_edges.remove_column('community_id')
                p_edges = p_edges.join(p_vertices, {'__dst_id':'__id'})
                num_p_edges = p_edges.num_rows()
                if not num_p_edges:
                    continue
                p_edges.remove_column('community_id')
                p_sgraph = gl.SGraph(vertices=p_vertices, edges=p_edges, vid_field='__id', 
                    src_field='__src_id', dst_field='__dst_id')
                partitions.append(p_sgraph)

            return self.child.find_communities(partitions=partitions)
        
        #bottom of the chain
        else:
            #push info up the chain
            ancestor = self.parent
            while ancestor:
                ancestor.g = gl.SGraph(vertices=ancestor.child.get_community_vertices(), 
                    edges=ancestor.child.get_community_edges(), vid_field='__id', src_field='__src_id', 
                    dst_field='__dst_id')

                ##to support multilevel detection > 2 levels: 
                #if ancestor.parent:
                #    randomly partition and run community detection

                ancestor = ancestor.parent
        return mdl

    def save(self, community_file_path, community_edge_file_path):
        #if supporting multilevel, save all but initial edges
        child = self.child
        while child.child:
            child = child.child
        child.g.vertices.save(community_file_path, format='csv')
        _g = gl.SGraph(edges=child.get_community_edges())
        _g.edges.save(community_edge_file_path, format='csv')

    @classmethod
    def load_vertices(cls, vertex_csv, header=False):
        sf = gl.SFrame.read_csv(vertex_csv, header=header, column_type_hints=str)
        assert sf.num_cols() in [2, 3], "vertex_csv must be 2 columns"
        col_names = ['__id', 'community_id']
        if sf.num_cols() == 3:
            col_names.append('description')
        rename = dict(zip(sf.column_names(), col_names))
        sf.rename(rename)
        sf['community_id'] = sf['community_id'].apply(lambda x: None if x == '\\N' else x)
        return sf

    @classmethod
    def load_edges(cls, edge_csv, header=False):
        sf = gl.SFrame.read_csv(edge_csv, header=header, column_type_hints=str)
        assert sf.num_cols() in [2,3], "edge_csv must be 2 or 3 columns"
        if sf.num_cols() == 2:
            sa = gl.SArray([1] * sf.num_rows())
            sf.add_column(sa, 'weight')
        col_names = ['__src_id', '__dst_id', 'weight']
        rename = dict(zip(sf.column_names(), col_names))
        sf.rename(rename)
        return sf

    @classmethod
    def run(cls, vertex_path, edge_path, output_dir=None):
        _start_time = datetime.now()

        vertices = cls.load_vertices(vertex_path)
        if 'description' in vertices.column_names():
            vertices.remove_column('description')
        edges = cls.load_edges(edge_path)
        gw = GraphWrapper(vertices, edges)

        hierarchy_levels = 2
        for i in range(hierarchy_levels - 1):
            gw = gw.get_community_gw()

        mdls = []
        iterations = 3
        for i in range(iterations):
            mdls.append(gw.find_communities())

        if output_dir:
            gw.save(output_dir+'community.csv', output_dir+'community_net.csv')
            
        print mdls
        print 'total runtime: %s' % (datetime.now() - _start_time)
        return gw, mdls

