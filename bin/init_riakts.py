#!/usr/bin/env python
import datetime, random
from riak import RiakClient, RiakError
random.seed(12345)

node_table_name="metric_nodes"
table_name="metrics"
family="graphite"
#="host1"

create = """
CREATE TABLE {table} (
	family		varchar   not null,
	series      varchar   not null,
	time        timestamp not null,
	metric      double,
	PRIMARY KEY (
		(family, series, quantum(time, 1, 'd')
	),family, series, time))
"""

rc = RiakClient(host='localhost', pb_port=8087)

create_q = create.format(table=table_name)
print create_q

secs = datetime.timedelta(0, 5)
start = datetime.datetime.now() - datetime.timedelta(0,3600)

try:
	rc.ts_query("","DESCRIBE %s" % table_name)
except RiakError:
	rc.ts_query(table_name, create_q)

try:
  rc.get_search_index(node_table_name)
except RiakError:
  rc.create_search_index(node_table_name)

bucket = rc.bucket(node_table_name)
bucket.set_properties({'search_index':node_table_name})