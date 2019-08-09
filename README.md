# dse-twcs-migrator
This program will migrate the data from a source STCS tabele to another table, which can be on a different cluster.  In order to run this program you will need a DSE cluster with DSE Analytics (Spark) enabled.

 


### How to build
To build simply run `mvn clean package` from the root of this project's directory.

After building the project, you should have an executable jar file in the target directory

eg: target/dse-twcs-migrator-0.1.jar

### TLDR
After compiling, simply edit the run-migration.sh file and change the values for source/target tables and ips.  The run-migration.sh file islocated in the root of this project. Refer to this file for reference of each parameter.  

### How to run

```
dse spark-submit \
    --conf spark.dse.cluster.migration.fromClusterHost="127.0.0.1" \
    --conf spark.dse.cluster.migration.toClusterHost="127.0.0.1" \
    --conf spark.dse.cluster.migration.fromKeyspace="sensors" \
    --conf spark.dse.cluster.migration.toKeyspace="sensors" \
    --conf spark.dse.cluster.migration.fromTable="tennant_tag_data_points_stcs" \
    --conf spark.dse.cluster.migration.toTable="tennant_tag_data_points_twcs" \
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
    --conf spark.replClassServer.port=38005 \
    --conf spark.rpc.askTimeout=800 \
    --conf spark.network.Timeout=800 \
    --class phact.MigrateTable target/dse-twcs-migrator-0.1.jar
  ```
    
### Spark Properties

Property|Meaning
---|---
spark.dse.cluster.migration.fromClusterHost | Initial contact point of the source cluster
spark.dse.cluster.migration.toClusterHost | Initial contact point of the target cluster
spark.dse.cluster.migration.fromKeyspace | Source Keyspace name
spark.dse.cluster.migration.toKeyspace | Target Keyspace name
spark.dse.cluster.migration.fromTable | Source table name
spark.dse.cluster.migration.toTable | Target table name
spark.dse.cluster.migration.newtableflag | Create the target table? (true or false)
spark.cassandra.input.consistency.level | Consistency level to read from the source cluster
spark.cassandra.output.consistency.level | Consistency level to write to the target cluster
spark.executor.memory |This specifies the amount of memory to use per executor. Because Spark processes data in memory, the more memory an executor has, the less frequent/likely data may spill to disk which incurs slow disk I/O. 
spark.executor.cores |This parameter tells spark how many concurrent tasks that can be run in each executor, and that usually means the number of CPUs to be assigned for each executor.
spark.cores.max |The maximum amount of CPU cores to request for the application from across the cluster (not from each machine). 
spark.driver.cores |Number of cores to use for the driver process, only in cluster mode.
spark.driver.memory |Amount of memory to use for the driver process.
spark.blockManager.port |Port for all the block managers to listen on.  These exist on both the driver and the executors
spark.broadcast.port |Port for the driver's HTTP server to listen on.
spark.driver.port |Port for the drivers to listen on.  This is used for communicating with the executors and the standalone master.
spark.executor.port |Port for the executor to listen on.  This is used for communicating with the driver.
spark.replClassServer.port |Port for the driver's HTTP class server to listen on.  This is only relevant for the Spark shell.
spark.rpc.askTimeout | Duration for the ask operation to wait before timing out.
spark.rpc.network.Timeout | Default timeout for all network interactions.




This project is a fork of:  https://github.com/phact/dse-cluster-migration
