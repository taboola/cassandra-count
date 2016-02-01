#!/bin/sh

cat src/make/cassandra-loader.sh build/libs/cassandra-count-uber*.jar > build/cassandra-count && chmod 755 build/cassandra-count
