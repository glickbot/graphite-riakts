#!/bin/bash

pushd `dirname $0` > /dev/null
BASE=`pwd`
popd > /dev/null

export PYTHONPATH=$BASE/lib:$BASE/var/graphite/lib:$BASE/env/lib
export PATH=$BASE/var/graphite/bin:/opt/graphite/bin:$PATH

carbon-client.py localhost:2004
