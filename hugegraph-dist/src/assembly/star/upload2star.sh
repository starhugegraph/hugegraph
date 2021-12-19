#!/bin/bash

version=$1

echo "Uploading to star-file: hugegraph-${version}.tar.gz"
curl -X PUT -u superstar:Superstar12345 -T hugegraph-${version}.tar.gz "http://10.14.139.8:8082/artifactory/star-file/hugegraph/hugegraph-${version}.tar.gz"