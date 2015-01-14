
import random
import graphlab as gl
from graphreduce import relaxmap
from datetime import datetime

#print gl.get_runtime_config()['GRAPHLAB_CACHE_FILE_LOCATIONS']

class GraphWrapper(object):

    def __init__(self, vertices=None, edges=None, child=None):
        self.parent = None
        self.child = child
        if vertices and edges:
            self.g = gl.SGraph(vertices=vertices, edges=edges, 
                vid_field='__id', src_field='__src_id', dst_field='__dst_id')

    AVG_COMM_SIZE = 400
    AVG_EDGES_PER_VERTEX = 200
    #todo: inspect available mem to set MAX_EDGES_IN_MEM
    MAX_EDGES_IN_MEM = 1e7
    PARTITION_LEN = MAX_EDGES_IN_MEM / (AVG_COMM_SIZE * AVG_EDGES_PER_VERTEX)
    PARTITION_MODE = True

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
        #print ' - Num homeless: %s' % self.g.vertices['community_id'].num_missing()
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
        grouped_edges = edges.groupby(['src_community_id', 'dst_community_id'], 
            {'sum':gl.aggregate.SUM('weight')})
        grouped_edges.rename({'src_community_id':'__src_id', 'dst_community_id':'__dst_id'})

        member_counts = self.g.vertices.groupby('community_id', 
            {'member_count':gl.aggregate.COUNT('__id')})
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

    def community_community_scores(self, distance_function):
        scores = self.g.edges.join(self.g.edges, {'__src_id':'__dst_id', '__dst_id':'__src_id'})
        scores.rename({'weight':'out_interest', 'weight.1':'in_interest'})
        scores['score'] = distance_function(scores)
        return scores

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
        return grouped_edges

    def community_user_links(self):
        edges = self.g.edges.join(self.g.vertices, {'__src_id':'__id'})
        edges.rename({'community_id':'src_community_id'})
        grouped_edges = edges.groupby(['__dst_id', 'src_community_id'], 
            {'sum':gl.aggregate.SUM('weight')})
        grouped_edges['weight'] = grouped_edges['sum']

        grouped_edges_max = grouped_edges.groupby('src_community_id', 
            {'max':gl.aggregate.MAX('weight')})
        grouped_edges = grouped_edges.join(grouped_edges_max, 'src_community_id')
        grouped_edges['weight'] = grouped_edges['weight'] / grouped_edges['max']

        grouped_edges.remove_column('sum')
        grouped_edges.remove_column('max')
        return grouped_edges

    _user_community_links = None
    _community_user_links = None
    def user_community_scores(self, distance_function):
        if not self._user_community_links:
            self._user_community_links = self.user_community_links()
        if not self._community_user_links:
            self._community_user_links = self.community_user_links()
        scores = self._user_community_links.join(self._community_user_links, 
            {'__src_id':'__dst_id', 'dst_community_id':'src_community_id'})
        scores.rename({'weight':'user_interest', 'weight.1':'community_interest'})
        scores.rename({'__src_id':'__id', 'dst_community_id':'community_id'})
        scores['score'] = distance_function(scores)
        #scores = scores.groupby('__id', {'score':gl.aggregate.CONCAT('community_id', 'score')})
        #scores = scores.unpack('score')
        return scores

    def get_community_gw(self):
        if self.child:
            raise Exception('Graphreduce only supports two level hierarchies at this time.')
        self.homes_for_the_homeless()
        gw = GraphWrapper(self.get_community_vertices(), self.get_community_edges(), child=self)
        self.parent = gw
        return gw

    def find_communities(self, partitions=None, just_once=False):
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
                print '   + relaxmap error, rerunning w/ 1 thread'
                _vertices, _mdl = relaxmap.find_communities(partition, 1)
            _n_communities = len(_vertices['community_id'].unique())
            print '   + found %s communities, mdl: %s' % (_n_communities, _mdl)

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
        self.g = gl.SGraph(vertices=vertices, edges=self.g.get_edges(), 
            vid_field='__id', src_field='__src_id', dst_field='__dst_id')
        self.homes_for_the_homeless()

        if just_once:
            return mdl

        if self.child and not self.PARTITION_MODE:
            return self.child.find_communities()

        #pull sgraph partitions out of child
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
                if len(_community_ids) < self.PARTITION_LEN and (i + 1) < num_grouped_vertices:
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

    def label_communities(self):
        frame = self.child.g.vertices.join(self.verticy_descriptions, 
            '__id', how='left')
        frame = frame.groupby('community_id', {
            "labels":gl.aggregate.CONCAT("description"),
            "member_count":gl.aggregate.COUNT("__id")})

        def remove_dups(_str):
            words = _str.split()
            return " ".join(sorted(set(words), key=words.index))
        frame['labels'] = frame['labels'].apply(
            lambda descriptions: ' '.join([remove_dups(x) for x in descriptions]))
        frame['labels'] = gl.text_analytics.count_words(frame['labels'])
        frame['labels'] = frame['labels'].dict_trim_by_values(3)
        stopwords = gl.text_analytics.stopwords()
        stopwords.update(['http', 'https'])
        frame['labels'] = frame['labels'].dict_trim_by_keys(stopwords, exclude=True)
        frame['labels'] = gl.text_analytics.tf_idf(frame['labels'])

        def label_score(row):
            new_scores = {}
            for label,value in row['labels'].items():
                new_scores[label] = value / row['member_count']
            return new_scores
        frame['labels'] = frame.apply(label_score)
        def top_labels(labels_dict):
            labels = sorted(labels_dict.items(), key=lambda x: x[1], reverse=True)[:5]
            return [x[0] for x in labels]
        frame['top_labels'] = frame['labels'].apply(top_labels)
        
        frame = self.g.vertices.join(frame, {'__id':'community_id'})
        self.g = gl.SGraph(vertices=frame, edges=self.g.get_edges(), 
            vid_field='__id', src_field='__src_id', dst_field='__dst_id')

    def search(self, search_terms):
        search_terms = [x.strip().lower() for x in search_terms]
        def _search(row):
            score = 0
            for search_term in search_terms:
                score += row['labels'].get(search_term, 0)
            if score:
                return {row['__id']:score * row['pr']}
            else:
                return None
        results = self.g.vertices.apply(_search, dtype=dict).dropna()
        if not results:
            return []
        results = gl.SFrame({'result':results}).stack('result', ['community_id', 'score'])
        results = self.g.vertices.join(results, {'__id':'community_id'})
        results = results.sort('score', ascending=False)
        return results

    def save(self, output_dir):
        self.child.g.save(output_dir+'child')
        self.g.save(output_dir+'parent')
        self.verticy_descriptions.save(output_dir+'verticy_descriptions')

    @classmethod
    def from_previous(cls, input_dir):
        parent = gl.load_sgraph(input_dir+'parent')
        verticy_descriptions = gl.load_sframe(input_dir+'verticy_descriptions')
        child = gl.load_sgraph(input_dir+'child')
        gw = cls()
        gw.g = parent
        gw.verticy_descriptions = verticy_descriptions
        gw.child = cls()
        gw.child.g = child
        return gw

    @classmethod
    def load_vertices(cls, vertex_csv, header=False):
        sf = gl.SFrame.read_csv(vertex_csv, header=header, column_type_hints=str, verbose=False)
        assert sf.num_cols() in [2, 5]
        col_names = ['__id', 'community_id']
        if sf.num_cols() == 5:
            col_names.append('description')
            col_names.append('screen_name')
            col_names.append('profile_image_url')
        rename = dict(zip(sf.column_names(), col_names))
        sf.rename(rename)
        sf['community_id'] = sf['community_id'].apply(lambda x: None if x == '\\N' else x)
        return sf

    @classmethod
    def load_edges(cls, edge_csv, header=False):
        sf = gl.SFrame.read_csv(edge_csv, header=header, column_type_hints=str, verbose=False)
        assert sf.num_cols() in [2,3], "edge_csv must be 2 or 3 columns"
        if sf.num_cols() == 2:
            sa = gl.SArray([1] * sf.num_rows())
            sf.add_column(sa, 'weight')
        col_names = ['__src_id', '__dst_id', 'weight']
        rename = dict(zip(sf.column_names(), col_names))
        sf.rename(rename)
        return sf

    @classmethod
    def reduce(cls, vertex_path, edge_path, output_dir=None):
        _start_time = datetime.now()

        verticy_descriptions = None
        vertices = cls.load_vertices(vertex_path)
        if 'description' in vertices.column_names():
            #keeping description around during detection hurts performance
            verticy_descriptions = gl.SFrame(vertices)
            verticy_descriptions.remove_column('community_id')
            vertices.remove_column('description')
            vertices.remove_column('screen_name')
            vertices.remove_column('profile_image_url')

        edges = cls.load_edges(edge_path)
        gw = GraphWrapper(vertices, edges)

        hierarchy_levels = 2
        for i in range(hierarchy_levels - 1):
            gw = gw.get_community_gw()

        if gw.child.g.edges.num_rows() < cls.MAX_EDGES_IN_MEM:
            cls.PARTITION_MODE = False
            iterations = 1
        else:
            iterations = 3

        mdls = []
        for i in range(iterations):
            mdls.append(gw.find_communities())
        gw.find_communities(just_once=True)
        pagerank = gl.pagerank.create(gw.g, verbose=False)['pagerank']
        gw.g.vertices['pr'] = pagerank['pagerank']

        if verticy_descriptions:
            gw.verticy_descriptions = verticy_descriptions
            gw.label_communities()

        if output_dir:
            gw.save(output_dir)
        
        print ''
        print mdls
        print 'total runtime: %s' % (datetime.now() - _start_time)
        return gw, mdls

