import traceback, datetime
from carbon.database import TimeSeriesDatabase
from riak import RiakClient


class RiakTSWriter(TimeSeriesDatabase):
  plugin_name = 'graphite_riakts'

  def __init__(self, option):
    print "Here we be initializing yon connections and things: %r" % option
    self.riak = RiakClient(host=option['RIAKTS_HOST'], pb_port=int(option['RIAKTS_PORT']))
    self.nodes_table = option['RIAKTS_NODES_TABLE']
    self.table_name = option['RIAKTS_TABLE_NAME']
    self.family = option['RIAKTS_FAMILY']
    self.nodes = self.riak.bucket(self.nodes_table)

  def write(self, metric, datapoints):
    try:

      #print "Here we be writing to %s the value of %r" % ( metric, datapoints )
      table = self.riak.table(self.table_name)

      rows = []
      for point in datapoints:
        time = datetime.datetime.utcfromtimestamp(point[0])
        rows.append([self.family, metric, time, float(point[1])])

      #print rows
      ts_obj = table.new(rows)
      #print vars(ts_obj)
      res = ts_obj.store()
      #print res
      return
    except Exception as e:
      raise RuntimeError("write error: %s" % (traceback.format_exc(e),))


  def exists(self, metric):
    try:
      key = "node-%s" % metric
      #print "Here we be checking if yon metric %s is existing" % metric
      o = self.nodes.get(key)
      if o.exists:
        #print "Yar, there be existing the metric of %s" % metric
        return True
      else:
        #print "Lo! The metric of %s be not here!" % metric
        o = self.nodes.new(key)
        branch = '.'.join(metric.split('.')[:-1])
        o.data = { "node_s": metric, "type_s": "node"}
#        print "Adding LEAF: %s" % metric
        o.store()
        while len(branch):
          bkey = "branch-%s" % branch
          b = self.nodes.get(bkey)
          if not b.exists:
            b.data = { "branch_s": branch, "type_s": "branch"}
#            print "Adding BRANCH: %s" % branch
            b.store()
          branch = '.'.join(branch.split('.')[:-1])

        return True
    except Exception as e:
      raise RuntimeError("exists error: %s" % (traceback.format_exc(e),))

  def create(self, metric, **options):
    try:
      #print "Here we be attempting yon creation of %s metric, with options: %r" % ( metric, options )
      # o = self.nodes.new(metric)
      # o.data = { "node_t" : metric }
      # o.store()
      return metric
    except Exception as e:
      raise RuntimeError("create error: %s" % (traceback.format_exc(e),))

  def get_metadata(self, metric, key):
    try:
      #print "Here we be getting yon metadata of metric %s and key %s" % ( metric, key )
      return True
    except Exception as e:
      raise RuntimeError("get_metadata error: %s" % (traceback.format_exc(e),))

  def set_metadata(self, metric, key, value):
    try:
      #print "Here we be setting yon metadata of metric %s and key %s with value %r" % ( metric, key, value )
      return True
    except Exception as e:
      raise RuntimeError("set_metadata error: %s" % (traceback.format_exc(e),))
  # def getFilesystemPath(self, metric):
  #   try:
  #     print "Here we be getting yon file system path of here metric %s" % metric
  #     return metric
  #   except (Exception) as e:
  #     raise RuntimeError("getFilesystemPath error: %s" % (traceback.format_exc(e),))