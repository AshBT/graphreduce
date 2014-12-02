
import random
import graphlab as gl
import relaxmap

class GraphWrapper(object):

    def __init__(self, vertices, edges, child=None):
        self.parent = None
        self.child = child
        #TODO: mk vertices optional
        edges = edges.join(vertices, {'__dst_id':'__id'})
        if 'community_id' in edges.column_names():
            edges.remove_column('community_id')
        self.g = gl.SGraph(vertices=vertices, edges=edges, 
            vid_field='__id', src_field='__src_id', dst_field='__dst_id')

    AVG_COMM_SIZE = 25

    def homes_for_the_homeless(self):
        unique = [x for x in list(self.g.vertices['community_id'].unique()) if x]
        if not unique:
            unique = ['1']
        if self.g.vertices['community_id'].num_missing() > (self.g.vertices.num_rows() * .5):
            distance = (self.g.vertices.num_rows() / self.AVG_COMM_SIZE) - len(unique)
            if distance > 0:
                offset = int(max(unique)) + 1
                unique += [str(x+offset) for x in range(int(distance))]
        self.g.vertices['community_id'] = self.g.vertices['community_id'].apply(lambda x: 
            random.choice(unique) if not x else x, skip_undefined=False)

    def get_community_vertices(self):
        unique = list(self.g.vertices['community_id'].unique())
        num_comms = len(unique) / self.AVG_COMM_SIZE if len(unique) > self.AVG_COMM_SIZE else 1
        num_comms = int(num_comms)
        comms = [str(x) for x in range(num_comms)]
        comms = [random.choice(comms) for x in unique]
        return gl.SFrame({'__id': unique, 'community_id':comms})

    def get_community_edges(self):
        #TODO: I suspect this can be done more efficiently w/ triple_apply
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
        gw = GraphWrapper(self.get_community_vertices(), self.get_community_edges(), child=self)
        self.parent = gw
        return gw

    def find_communities(self, partitions=None):
        if not partitions:
            partitions = [self.g]

        print '**starting community detection, %s partition(s)' % len(partitions)

        mdl = 0
        vertices = None
        for partition in partitions:
            print '**use partition vertex count: %s' % partition.vertices.num_rows()
            _vertices, _mdl = relaxmap.find_communities(partition)
            mdl += _mdl
            if not vertices:
                vertices = _vertices
            else:
                _offset = len(vertices['community_id'].unique())
                _vertices['community_id'] = _vertices['community_id'].apply(
                    lambda x: str(int(x) + _offset))
                vertices = vertices.append(_vertices)
                assert len(vertices['community_id'].unique()) - len(_vertices['community_id'].unique()) == _offset

        self.g = gl.SGraph(vertices=vertices, edges=self.g.get_edges(), 
            vid_field='__id', src_field='__src_id', dst_field='__dst_id')
        self.homes_for_the_homeless()
        
        if self.parent: 
            self.parent.g = gl.SGraph(edges=self.get_community_edges(), 
                src_field='__src_id', dst_field='__dst_id')

        #pull sgraph partitions out of child
        if self.child:
            partitions = []
            grouped_vertices = vertices.groupby('community_id', {'__ids': gl.aggregate.CONCAT('__id')})
            for row in grouped_vertices:
                vertices = gl.SFrame({'__id':row['__ids']})
                print '**mk partition vertex count: %s' % vertices.num_rows()
                p_vertices = self.child.g.vertices.join(vertices, {'community_id':'__id'})
                if not p_vertices.num_rows():
                    continue
                p_edges = self.child.g.edges.join(p_vertices, {'__src_id':'__id'})
                if not p_edges.num_rows():
                    continue
                p_edges.remove_column('community_id')
                p_edges = p_edges.join(p_vertices, {'__dst_id':'__id'})
                if not p_edges.num_rows():
                    continue
                p_edges.remove_column('community_id')
                partitions.append(gl.SGraph(vertices=p_vertices, edges=p_edges, 
                    vid_field='__id', src_field='__src_id', dst_field='__dst_id'))
            return self.child.find_communities(partitions=partitions)
        return mdl

    def save(self):
        """
        Call save on all children as well
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
