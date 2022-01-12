FROM ubuntu:xenial

LABEL maintainer="HugeGraph Docker Maintainers"

# 1. Install needed dependencies of GraphServer & RocksDB
RUN set -x \
    && apt-get -q update \
    && apt-get -q install -y --no-install-recommends --no-install-suggests \
       curl \
       lsof \
       g++ \
       gcc \
    && apt-get clean

# 2. Init HugeGraph Sever
ADD ./jdk1.8.0_271.tar.gz /root
ADD ./hugegraph-3.0.0.tar.gz /root/hugegraph-server
#ADD ./config /root/.kube/config
RUN set -e \
    && cd /root/hugegraph-server/hugegraph-3.0.0 \
    && sed -i "s/^restserver.url.*$/restserver.url=http:\/\/0.0.0.0:8080/g" ./conf/rest-server.properties \
    && sed -n '134p' ./bin/start-hugegraph.sh | grep "&" > /dev/null && sed -i 134{s/\&$/#/g} ./bin/start-hugegraph.sh \
    && sed -n '144p' ./bin/start-hugegraph.sh | grep "exit" > /dev/null && sed -i 144{s/^/#/g} ./bin/start-hugegraph.sh \
    && ./bin/init-store.sh

EXPOSE 8080
WORKDIR /root
VOLUME /root

#ENTRYPOINT ["/bin/bash", "-c", "while true;do echo hello docker;sleep 1;done"]
ENTRYPOINT ["/root/hugegraph-server/hugegraph-3.0.0/bin/start-hugegraph.sh"]