#!/bin/bash

pushd `dirname $0` > /dev/null
BASE=`pwd`
popd > /dev/null

export PYTHONPATH=$BASE/lib:$BASE/var/graphite/lib:$BASE/env/lib

#./test.py
./bin/carbon-cache.py --config=$BASE/conf/carbon.conf --nodaemon writer start
