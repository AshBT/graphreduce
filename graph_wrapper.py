
import random
import graphlab as gl
import relaxmap

class GraphWrapper(object):

    def __init__(self, vertices, edges, child=None):
        self.child = child
        edges = edges.join(vertices, {'__dst_id':'__id'})
        edges.remove_column('community_id')
        self.g = gl.SGraph(vertices=vertices, edges=edges, 
            vid_field='__id', src_field='__src_id', dst_field='__dst_id')

    AVG_COMM_SIZE = 25

    def homes_for_the_homeless(self):
        comm = self.g.vertices['community_id']
        if comm.num_missing() > (self.g.vertices.num_rows() * .5):
            raise Exception('not implemented yet')
        unique = list(comm.unique())
        self.g.vertices['community_id'] = self.g.vertices['community_id'].apply(lambda x: 
            random.choice(unique) if not x else x, skip_undefined=False)

    def do_pagerank(self):
        """"""

    def get_community_vertices(self):
        comm = self.g.vertices['community_id']
        unique = list(comm.unique())
        if len(unique) < self.AVG_COMM_SIZE:
            raise Exception('not implemented yet')
        super_comms = [str(x) for x in range(len(unique) / self.AVG_COMM_SIZE)]
        super_comms = [random.choice(super_comms) for x in unique]
        return gl.SFrame({'__id': unique, 'community_id': super_comms})

    def get_community_edges(self):
        edges = self.g.edges.join(self.g.vertices, {'__dst_id':'__id'})
        edges.rename({'community_id':'dst_community_id'})
        edges = edges.join(self.g.vertices, {'__src_id':'__id'})
        edges.rename({'community_id':'src_community_id'})
        grouped_edges = edges.groupby(['src_community_id', 'dst_community_id'], 
            {'sum':gl.aggregate.SUM('weight')})
        grouped_edges.rename({'src_community_id':'__src_id', 'dst_community_id':'__dst_id'})
        grouped_grouped_edges = grouped_edges.groupby('__src_id', 
            {'max':gl.aggregate.MAX('sum')})
        grouped_edges = grouped_edges.join(grouped_grouped_edges, '__src_id')
        grouped_edges['weight'] = grouped_edges['sum'] / grouped_edges['max']
        grouped_edges.remove_column('sum')
        grouped_edges.remove_column('max')
        return grouped_edges

    def get_community_gw(self):
        self.homes_for_the_homeless()
        return GraphWrapper(self.get_community_vertices(), 
            self.get_community_edges(), child=self)

    def find_communities(self, partitions=None):

        if not partitions:
            partitions = [self.g]

        print 'starting community detection, %s partition(s)' % len(partitions)

        mdl = 0
        vertices = None
        for partition in partitions:
            print 'use partition v count: %s' % partition.vertices.num_rows()
            _vertices, _mdl = relaxmap.find_communities(partition)
            mdl += _mdl
            if not vertices:
                vertices = _vertices
            else:
                _offset = vertices.num_rows()
                _vertices['community_id'] = _vertices['community_id'].apply(
                    lambda x: str(int(x) + _offset))
                vertices = vertices.append(_vertices)
        
        print 'vertices.num_rows %s' % vertices.num_rows()
        print 'self.g.vertices.num_rows %s' %  self.g.vertices.num_rows()

        #update self.g.vertices w/ community info
        vertices.sort('__id')
        self.g.vertices.sort('__id')
        self.g.vertices['community_id'] = vertices['community_id']
        
        #pull sgraph partitions out of child
        if self.child:
            partitions = []
            grouped_vertices = vertices.groupby('community_id', {'__ids': gl.aggregate.CONCAT('__id')})
            for row in grouped_vertices:
                vertices = gl.SFrame({'__id':row['__ids']})
                print 'mk partition v count: %s' % vertices.num_rows()
                p_vertices = self.child.g.vertices.join(vertices, {'community_id':'__id'})
                p_edges = self.child.g.edges.join(p_vertices, {'__src_id':'__id'})
                p_edges.remove_column('community_id')
                p_edges = p_edges.join(p_vertices, {'__dst_id':'__id'})
                p_edges.remove_column('community_id')
                partitions.append(gl.SGraph(vertices=p_vertices, edges=p_edges, 
                    vid_field='__id', src_field='__src_id', dst_field='__dst_id'))
            return self.child.find_communities(partitions=partitions)
        return mdl

    def save(self):
        """
        Call save on all childs as well
        """

    @classmethod
    def load_vertices(cls, vertex_csv, header=False):
        sf = gl.SFrame.read_csv(vertex_csv, header=header)
        assert sf.num_cols() in [2], "vertex_csv must be 2 columns"
        col_names = ['__id', 'community_id']
        rename = dict(zip(sf.column_names(), col_names))
        sf.rename(rename)
        sf['community_id'] = sf['community_id'].apply(lambda x: None if x == '\\N' else x)
        return sf

    @classmethod
    def load_edges(cls, edge_csv, header=False):
        sf = gl.SFrame.read_csv(edge_csv, header=header)
        assert sf.num_cols() in [2,3], "edge_csv must be 2 or 3 columns"
        if sf.num_cols() == 2:
            sa = gl.SArray([1] * sf.num_rows())
            sf.add_column(sa, 'weight')
        col_names = ['__src_id', '__dst_id', 'weight']
        rename = dict(zip(sf.column_names(), col_names))
        sf.rename(rename)
        return sf
