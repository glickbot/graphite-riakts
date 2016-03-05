#!/bin/bash

pushd `dirname $0` > /dev/null
BASE=`pwd`
popd > /dev/null

export PYTHONPATH=$BASE/lib:$BASE/var/graphite/lib:$BASE/env/lib
export GRAPHITE_API_CONFIG=$BASE/conf/graphite.conf
export BIND_ADDRESS=0.0.0.0

./bin/graphite.py
