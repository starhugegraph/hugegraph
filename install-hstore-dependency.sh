#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

export MAVEN_HOME=/home/scmtools/buildkit/maven/apache-maven-3.3.9/
export JAVA_HOME=/home/scmtools/buildkit/java/jdk1.8.0_25/
export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH
TRAVIS_DIR=./hugegraph-dist/src/assembly/travis/lib
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-pd-client-1.0-SNAPSHOT.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-client -Dversion=1.0-SNAPSHOT -Dpackaging=jar  -DpomFile=$TRAVIS_DIR/hg-pd-client-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-pd-common-1.0-SNAPSHOT.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-common -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-pd-common-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-pd-grpc-1.0-SNAPSHOT.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-grpc -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-pd-grpc-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-store-client-1.0-SNAPSHOT.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-client -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-store-client-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-store-grpc-1.0-SNAPSHOT.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-grpc -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-store-grpc-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-store-term-1.0.1.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-term -Dversion=1.0.1 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-store-term-pom.xml

