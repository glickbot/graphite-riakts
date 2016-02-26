#!/bin/bash

pushd `dirname $0` > /dev/null
BASE=`pwd`
popd > /dev/null

export PYTHONPATH=$BASE/lib:$BASE/var/graphite/lib:$BASE/env/lib
export GRAPHITE_API_CONFIG=$BASE/conf/graphite.conf

./bin/graphite.py
