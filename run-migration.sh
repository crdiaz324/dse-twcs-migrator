#!/bin/bash 

# This script will run kick off the spark job to migrate data from a source cluster
# to a target cluster. 

# you must fill in the following parameters accordingly:
SOURCE_IP=
TARGET_IP=
SOURCE_KS=
TARGET_KS=
SOURCE_TBL=
TARGET_TBL=


dse spark-submit \
    --conf spark.dse.cluster.migration.fromClusterHost="${SOURCE_IP}" \
    --conf spark.dse.cluster.migration.toClusterHost="${TARGET_IP}" \
    --conf spark.dse.cluster.migration.fromKeyspace="${SOURCE_KS}" \
    --conf spark.dse.cluster.migration.toKeyspace="${TARGET_KS}" \
    --conf spark.dse.cluster.migration.fromTable="${SOURCE_TBL}" \
    --conf spark.dse.cluster.migration.toTable="${TARGET_TBL}" \
    --conf spark.dse.cluster.migration.newtableflag="false" \
    --conf spark.cassandra.input.consistency.level=LOCAL_QUORUM \
    --conf spark.cassandra.output.consistency.level=LOCAL_ONE \
    --conf spark.executor.memory=8G \
    --conf spark.executor.cores=2 \
    --conf spark.cores.max=12 \
    --conf spark.driver.cores=2 \
    --conf spark.driver.memory=8G \
    --conf spark.blockManager.port=38000 \
    --conf spark.broadcast.port=38001 \
    --conf spark.driver.port=38002 \
    --conf spark.executor.port=38003 \
    --conf spark.fileserver.port=38004 \
    --conf spark.replClassServer.port=38005 \
    --conf spark.rpc.askTimeout=800 \
    --conf spark.network.Timeout=800 \
    --class phact.MigrateTable target/dse-twcs-migrator-0.1.jar
