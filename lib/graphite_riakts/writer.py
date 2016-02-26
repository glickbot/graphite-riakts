import traceback, datetime
from carbon.database import TimeSeriesDatabase
from riak import RiakClient, RiakError

class RiakTSWriter(TimeSeriesDatabase):
  plugin_name = 'graphite_riakts'

  def __init__(self, option):
    print "Here we be initializing yon connections and things: %r" % option
    self.riak = RiakClient(host='localhost', pb_port=8087)
    self.nodes_table = 'metric_nodes'
    self.table_name = 'metrics'
    self.family = 'graphite'
    self.nodes = self.riak.bucket(self.nodes_table)

  def write(self, metric, datapoints):
    try:
      print "Here we be writing to %s the value of %r" % ( metric, datapoints )
      table = self.riak.table(self.table_name)

      rows = []
      for point in datapoints:
        time = datetime.datetime.utcfromtimestamp(point[0])
        rows.append([self.family, metric, time, float(point[1])])

      print rows
      ts_obj = table.new(rows)
      print vars(ts_obj)
      res = ts_obj.store()
      print res
      return True
    except (Exception) as e:
      raise RuntimeError("write error: %s" % (traceback.format_exc(e),))


  def exists(self, metric):
    try:
      print "Here we be checking if yon metric %s is existing" % metric
      o = self.nodes.get(metric)
      if o.exists:
        print "Yar, there be existing the metric of %s" % metric
        return True
      else:
        print "Lo! The metric of %s be not here!" % metric
        o = self.nodes.new(metric)
        o.data = { "node_s" : metric, "branch_s" : '.'.join(metric.split('.')[:-1]) }
        o.store()
        return metric
    except (Exception) as e:
      raise RuntimeError("exists error: %s" % (traceback.format_exc(e),))


  def create(self, metric, **options):
    try:
      print "Here we be attempting yon creation of %s metric, with options: %r" % ( metric, options )
      o = self.nodes.new(metric)
      o.data = { "node_t" : metric }
      o.store()
      return metric
    except (Exception) as e:
      raise RuntimeError("create error: %s" % (traceback.format_exc(e),))


  def get_metadata(self, metric, key):
    try:
      print "Here we be getting yon metadata of metric %s and key %s" % ( metric, key )
      return True
    except (Exception) as e:
      raise RuntimeError("get_metadata error: %s" % (traceback.format_exc(e),))


  def set_metadata(self, metric, key, value):
    try:
      print "Here we be setting yon metadata of metric %s and key %s with value %r" % ( metric, key, value )
      return True
    except (Exception) as e:
      raise RuntimeError("set_metadata error: %s" % (traceback.format_exc(e),))