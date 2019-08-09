package phact 

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.scheduler.{SparkListener,  SparkListenerTaskEnd}
import com.datastax.spark.connector.writer._


// For DSE it is not necessary to set connection parameters for spark.master (since it will be done
// automatically)
object MigrateTable extends App {

  val spark = SparkSession.builder
    .appName("dse-twcs-migrator")
    .enableHiveSupport()
    .getOrCreate()

  var conf = spark.sparkContext.getConf


  var clusterHostOne        = conf.get("spark.dse.cluster.migration.fromClusterHost", null)
  var clusterHostTwo        = conf.get("spark.dse.cluster.migration.toClusterHost", null)
  var fromKeyspace          = conf.get("spark.dse.cluster.migration.fromKeyspace", null)
  var fromTable             = conf.get("spark.dse.cluster.migration.fromTable", null)
  var toKeyspace            = conf.get("spark.dse.cluster.migration.toKeyspace", fromKeyspace)
  var toTable               = conf.get("spark.dse.cluster.migration.toTable", fromTable)
  var newTableFlag          = conf.get("spark.dse.cluster.migration.newtableflag", "false").toBoolean
  var fromuser              = conf.get("spark.dse.cluster.migration.fromuser", null)
  var frompassword          = conf.get("spark.dse.cluster.migration.frompassword", null)
  var touser                = conf.get("spark.dse.cluster.migration.touser", null)
  var topassword            = conf.get("spark.dse.cluster.migration.topassword", null)
  var batchingColumn        = conf.get("spark.dse.cluser.migration.batchingColumn", null)
  var timestampColumn       = conf.get("spark.dse.cluser.migration.timestampColumn", null)
  var ttlColumn             = conf.get("spark.dse.cluser.migration.ttlColumn", null)



  var connectorToClusterOne : CassandraConnector = _
  var connectorToClusterTwo : CassandraConnector  = _

  import spark.implicits._

  if (fromuser != null && frompassword != null) {
    connectorToClusterOne = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", clusterHostOne).set("spark.cassandra.auth.username", fromuser).set("spark.cassandra.auth.password", frompassword))
  }else{
    connectorToClusterOne = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", clusterHostOne))
  }
  if (touser != null && topassword != null) {
    connectorToClusterTwo = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", clusterHostTwo).set("spark.cassandra.auth.username", touser).set("spark.cassandra.auth.password", topassword))
  }else{
    connectorToClusterTwo = CassandraConnector(spark.sparkContext.getConf.set("spark.cassandra.connection.host", clusterHostTwo))
  }

  val rddFromClusterOne = {
    // Sets connectorToClusterOne as default connection for everything in this code block
    implicit val c = connectorToClusterOne
    spark.sparkContext.cassandraTable(fromKeyspace,fromTable)
  }

  // Need to figure out how to get the column names dynamically and add the additional two columns (writetime and ttl) to this RDD
  val rddWithWtAndTTL = rddFromClusterOne.
    select("tnt", "thsh", "intvl", "ahsh", "ver", "ioff", "attrs", "del", "q", "tag", "type", "val", "attrs".writeTime as "writetime", "attrs".ttl as "timetolive")

  val stcsTable = spark.
    read.
    format("org.apache.spark.sql.cassandra").
    options(Map( "table" -> fromTable, "keyspace" -> fromKeyspace)).
    load()

    stcsTable.registerTempTable("stcsTable")


  // The following line may be to expensive to run on the source cluster.
  // But it would help to know the number or rows to transfer to compare with the final count.
  // println("Count from source cluster: " + rddFromClusterOne.cassandraCount())

  // We can also use nodetool tablestats -- ks.table to get a rough estimate.
  {
    //Sets connectorToClusterTwo as the default connection for everything in this code block
    implicit val c = connectorToClusterTwo
    /* Start */
    var recordsWrittenCount = 0L
    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        synchronized {
          recordsWrittenCount += taskEnd.taskMetrics.outputMetrics.recordsWritten
        }
      }
    })
    /* End */
    if (newTableFlag) {
      var tableDef = TableDef(toKeyspace,toTable, rddFromClusterOne.tableDef.partitionKey, rddFromClusterOne.tableDef.clusteringColumns, rddFromClusterOne.tableDef.regularColumns, rddFromClusterOne.tableDef.indexes, false)
      rddFromClusterOne.saveAsCassandraTableEx(tableDef)
    } else {
      //rddWithWtAndTTL.saveToCassandra(toKeyspace, toTable, writeConf = WriteConf(timestamp = TimestampOption.perRow("writetime"), ttl = TTLOption.perRow("timetolive")))
      val dfSortedIntvl = spark.sql("select distinct(intvl) from stcsTable order by 1")
      dfSortedIntvl.collect.
        foreach( r => rddWithWtAndTTL.
          filter(x => (x.getInt("intvl").
          equals((r.getInt(0))) ) ).
          saveToCassandra("sensors", "twcs4", writeConf = WriteConf(timestamp = TimestampOption.perRow("writetime"), ttl = TTLOption.perRow("timetolive"))) )

    }

    // Enable the following 5 lines to ge t a count from the target cluster.
    //println("Records written to target cluster: " + recordsWrittenCount)
    //val rddFromClusterTwo = {
    //  implicit val c = connectorToClusterTwo
    //  spark.sparkContext.cassandraTable(toKeyspace,toTable)
    //}
    //println("Count from target cluster: " + rddFromClusterTwo.cassandraCount())
  }

  spark.stop()
  sys.exit(0)
}
