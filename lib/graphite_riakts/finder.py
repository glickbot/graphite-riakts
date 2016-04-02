import time
import re
import pandas as pd

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

from riak import RiakClient
from datetime import datetime, timedelta


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
        #print vars(query)
        bucket = self.riak.bucket_type('default').bucket('metric_nodes')

        exact = bucket.get(query.pattern)
        nodes = []
        if exact.exists:
            yield LeafNode(query.pattern,RiakTSReader(query.pattern, self.riak, self.config))
        else:
            pattern = query.pattern
            pattern = re.sub('\.select metric','',pattern)
            if re.match('^[^*]*$', pattern):
              pattern = re.sub('\.*$', '.*', pattern)
            pattern = re.sub('\*','[^.]*', pattern)
            print "Solr pattern: %s" % pattern
            results = bucket.search("branch_s:/%s/" % pattern, index='metric_nodes', rows=1000000)
            #print "Branch search results"
            print(results)
            for doc in results['docs']:
                branch = bucket.get(doc['_yz_rk'])
                branch_node = BranchNode(branch.data['branch_s'])
                #print "BranchNode: name: %s, path: %s" % (branch_node.name, branch_node.path)
                yield branch_node
            node_results = bucket.search("node_s:/%s/" % pattern, index='metric_nodes', rows=1000000)
            #print "Node search results"
            print(node_results['docs'])
            for doc in node_results['docs']:
                node = bucket.get(doc['_yz_rk'])
                node_name = node.data['node_s']
                yield LeafNode(node_name, RiakTSReader(node_name, self.riak, self.config))


class RiakTSReader(object):
    __slots__ = ('node', 'riak', 'config')

    def __init__(self, node, riak, config):
        self.node = node
        self.riak = riak
        self.config = config

    def get_intervals(self):
        return IntervalSet([Interval(0, int(time.time()))])

    def fetch(self, startTime, endTime):
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
            startTime = endTime - timedelta(1)
        start = datetime.fromtimestamp(startTime)
        end = datetime.fromtimestamp(endTime)
        max_span = timedelta(0, int(quanta) * 3)
        cursor = start
        print "[%s] %s - %s: Gathering..." % (self.node, start, end)
        while cursor < end:
            if cursor + max_span > end:
                next_cursor = end
            else:
                next_cursor = cursor + max_span

            q = select.format(table=table,family=family,series=self.node,t1=dt_to_ms(cursor),t2=dt_to_ms(next_cursor))
            #print q
            results = self.riak.ts_query(table,q)

            rows.extend(results.rows)
            print "[%s] %s - %s: %d" % (self.node, cursor, next_cursor, len(results.rows))
            cursor = next_cursor

        # PANDA HELP
        df = pd.DataFrame(rows)
        ts = df.set_index(0)
        #ts.to_csv('%s.csv' % self.node)
        # Rounding to nearest timestep
        ts.index = ts.index.round("%sS" % timestep).snap("%sS" % timestep)
        # deduplicating potential collisions due to rounding
        ts = ts[~ts.index.duplicated(keep='last')]
        # Creating all steps
        index = pd.date_range(start=ts.index[0], end=ts.index[-1], freq="%sS" % timestep)
        # Reindexing values, filling in blankes
        try:
            fixed = ts.reindex(index, fill_value=None)
        except ValueError as e:
            print "!!! Value Error !!!: %s" % e
            fixed = ts
        # for debugging
        # fixed.to_csv("%s-fixed.csv" % self.node)

        try:
            #first_time = dt_to_timestamp(rows[0][0])
            #last_time = dt_to_timestamp(rows[-1][0])
            #values = [row[1] for row in rows]
            first_time = fixed.index[0].value // 10**9
            last_time = fixed.index[-1].value // 10**9
            time_info = (first_time,last_time,timestep)
            values = fixed.values
        except IndexError:
            print "!!! INDEX ERROR !!!"
            time_info = (dt_to_timestamp(start),dt_to_timestamp(end),timestep)
            values = []

        return time_info, values

