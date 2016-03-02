#!/bin/bash

cd download
wget https://grafanarel.s3.amazonaws.com/builds/grafana_2.6.0_amd64.deb
apt-get install -y adduser libfontconfig
dpkg -i grafana_2.6.0_amd64.deb
cd ..
