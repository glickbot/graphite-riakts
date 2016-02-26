import itertools
import time
import re

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

from riak import RiakClient
from datetime import datetime, timedelta

def branch_query(pattern):
    pattern = re.sub('\.','\.',pattern)
    return "/" + ".*".join(pattern.split('*')) + "/"

def node_query(pattern):
    pattern = re.sub('\.','\.',pattern)
    return "/" + "[^.]*".join(pattern.split('*')) + "/"

def maybe_add_wild(pattern):
    pattern = re.sub('\.select metric','',pattern)
    if re.match('\*', pattern):
        return pattern
    else:
        return re.sub('[.*]*$', '', pattern) + ".*"

def dt_to_ms(dt):
    td = dt - datetime.utcfromtimestamp(0)
    return int(td.total_seconds() * 1000.0)

def dt_to_timestamp(dt):
    td = dt - datetime.utcfromtimestamp(0)
    return int(td.total_seconds())

class RiakTSFinder(object):
    def __init__(self, config):
        self.config = config
        self.riak = RiakClient(host=config['riak_ts']['host'],port=config['riak_ts']['port'])

    def find_nodes(self, query):
        print vars(query)
        bucket = self.riak.bucket_type('default').bucket('metric_nodes')

        exact = bucket.get(query.pattern)
        nodes = []
        if exact.exists:
            yield LeafNode(query.pattern,RiakTSReader(query.pattern, self.riak, self.config))
        else:
            # find branches
            #print "Before MAW: %s" % query.pattern
            pattern = maybe_add_wild(query.pattern)
            #print "After MAW: %s" % pattern
            solr_query = branch_query(pattern)
            #print "Finding branches with query: %s" % solr_query
            results = bucket.search("branch_s:%s" % solr_query, index='metric_nodes')
            # print(results['docs'][0]['name_s'])
            print "Branch search results"
            print(results)
            if len(results['docs']) > 0:
                root_len = len(pattern.split('.')) - 1
                branches = {}
                for doc in results['docs']:
                    branch = bucket.get(doc['_yz_rk'])
                    print branch.data
                    b_name = branch.data['branch_s'].split('.')[root_len:root_len + 1][0]
                    branches[b_name] = 1
                    print b_name
                for b_name in branches:
                    yield BranchNode(b_name)
            else:
                solr_query = node_query(query.pattern)
                node_results = bucket.search("node_s:%s" % solr_query, index='metric_nodes')
                print "Node search results"
                print(node_results['docs'])
                if len(node_results) > 0:
                    for doc in node_results['docs']:
                        yield LeafNode(doc['_yz_rk'],RiakTSReader(doc['_yz_rk'], self.riak, self.config))
                else:
                    print "Object not found"

class RiakTSReader(object):
    __slots__ = ('node','riak','config')

    def __init__(self, node, riak, config):
        self.node = node
        self.riak = riak
        self.config = config

    def get_intervals(self):
        return IntervalSet([Interval(0, int(time.time()))])

    def fetch(self, startTime, endTime):
        print "WHAT"
        print "RiakTSReader.fetch(%s,%s) for node %s" % (startTime,endTime,self.node)
        select = """
select time, metric from {table} where
family = '{family}' and series = '{series}' and
time > {t1} and time < {t2}
"""

        quanta = self.config['riak_ts']['quanta_seconds']
        table = self.config['riak_ts']['table']
        family = self.config['riak_ts']['family']
        timestep = self.config['riak_ts']['timestep']

        rows = []
        if endTime == None:
            endTime = datetime.now()
        if startTime == None:
            startTime = end - timedelta(1)
        start = datetime.fromtimestamp(startTime)
        end = datetime.fromtimestamp(endTime)
        max_span = timedelta(0, int(quanta) * 3)
        cursor = start
        while cursor < end:
            if cursor + max_span > end:
                next_cursor = end
            else:
                next_cursor = cursor + max_span

            q = select.format(table=table,family=family,series=self.node,t1=dt_to_ms(cursor),t2=dt_to_ms(next_cursor))
            print q
            results = self.riak.ts_query(table,q)

            rows.extend(results.rows)
            cursor = next_cursor


        print rows

        try:
            first_time = dt_to_timestamp(rows[0][0])
            last_time = dt_to_timestamp(rows[-1][0])
            time_info = (first_time,last_time,timestep)
            values = [row[1] for row in rows]
        except IndexError:
            time_info = (dt_to_timestamp(start),dt_to_timestamp(end),timestep)
            values = []

        return time_info, values

