import org.apache.spark.{SparkConf , SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType ,LongType , IntegerType , StructField, StructType}
import scala.collection.immutable.List
import org.apache.hadoop.hbase.{HBaseConfiguration , HTableDescriptor , TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import scala.util.Try
import org.slf4j.LoggerFactory
import org.apache.log4j.{Logger , Level}


object TwitterTrends {
  
  @transient lazy val logger=LoggerFactory.getLogger(this.getClass())
  
  var conn: Connection = _

  
  def main(args: Array[String]): Unit = {

    val twProperties = Utils.getTwitterProperties();

    System.setProperty("twitter4j.oauth.consumerKey", twProperties.getProperty("CONSUMER_KEY"))
    System.setProperty("twitter4j.oauth.consumerSecret", twProperties.getProperty("CONSUMER_SECRET"))
    System.setProperty("twitter4j.oauth.accessToken", twProperties.getProperty("ACCESS_TOKEN"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", twProperties.getProperty("ACCESS_TOKEN_SECRET"))

    Logger.getLogger("org").setLevel(Level.ERROR);

    val sparkConf = new SparkConf()
      .setAppName("realtime-tracking-popular-hashTags")
      .setMaster("local[*]")
      .set("spark.ui.port", "4040")
      .set("spark.streaming.blockInterval", "1000ms")

    val streamingContext = new StreamingContext(sparkConf, Seconds(1));

    streamingContext.sparkContext.setLogLevel("OFF");

    val receiverTweetDStream = TwitterUtils.createStream(streamingContext, None)

    val statuses = receiverTweetDStream.map(status => status.getText)
    val hashTags = statuses.flatMap(_.split(" ")).filter(parseHashtag)
    val hashTagKeyValue = hashTags.map((_, 1))

    val hashTagsCounts: DStream[(String, Long)] = hashTagKeyValue
      .reduceByKey(_ + _)
      .updateStateByKey(aggregate_hashtags_count)

    
    // ############################################ Option one but in single-node sometimes it becomes slow #################################### //
    /*
    hashTagsCounts.foreachRDD { (rdd : RDD[(String, Long)], time: Time) =>

      if (!rdd.isEmpty()) {

        val topHashTags = getTopHashtags(rdd, time)
        // try to post to flask server
        val labels = topHashTags.map(_._1).toList
        val data = topHashTags.map(_._2.toString()).toList
        Utils.post_to_flaskDashboard("http://localhost:5000/updateData", labels, data)

        rdd.foreachPartition { partitionOfRecords =>

          val hbaseConnection = getConnection
          val table = hbaseConnection.getTable(TableName.valueOf("hashtags_trend"));

          def send2Hbase(row: (String, Long)) = {

            val put = new Put(row._1.getBytes())
            put.addColumn("hashtags_count".getBytes(), "count".getBytes(), row._2.toString().getBytes());
            table.put(put)
          }

          partitionOfRecords.foreach { row =>

            if (row._2 > 1) {
              send2Hbase(row)

            }

          }

        }

      }
    }
  */

    
    
    hashTagsCounts.foreachRDD { (rdd: RDD[(String, Long)], time: Time) =>

      if (!rdd.isEmpty()) {

        val topHashTags = getTopHashtags(rdd, time)

        // try to post to flask server
        val labels = topHashTags.map(_._1).toList
        val data = topHashTags.map(_._2.toString()).toList
        Utils.post_to_flaskDashboard("http://localhost:5000/updateData", labels, data)

        // try to push data to hbase
        topHashTags.foreach(send2Hbase(table_name = "hashtags_trend", _))

        println("data pushed successfully to hbase")

      }

    }

   
     
      streamingContext.checkpoint("/home/hdpadmin/workspace/mytwitterApp/src/main")
      streamingContext.start()
      streamingContext.awaitTermination()
    
    
    
   }

  //######################### My Custom Stateful Transformation Function ################################//

  def aggregate_hashtags_count(newData: Seq[Int], state: Option[Long]) = {

    val current = newData.foldLeft(0L)(_ + _)
    val prev = state.getOrElse(0L)

    Some(current + prev)

  }

  def parseHashtag(hashtag: String): Boolean =
    {

      {
        !hashtag.isEmpty()
      } &&
        {
          hashtag.startsWith("#")

        }

    }

  // #########################  Load processed data in low latency data store  ###############################//

  
     def send2Hbase(table_name:String,row:(String , Long)) :Unit =
     {

       val table = getConnection.getTable(TableName.valueOf(table_name));
       val put = new Put(row._1.getBytes())

       put.addColumn("hashtags_count".getBytes(), "count".getBytes(), row._2.toString().getBytes());
       table.put(put);


     }
      

  // ###########################  Visualize data in real-live Dashboard  ##################################//

  def getTopHashtags(rdd: RDD[(String, Long)], time: Time): Array[(String, Long)] = {

    val spark = new SQLContext(rdd.context)
    val row_rdd = rdd.map(record => Row(record._1, record._2))

    val schema = StructType(Array(
      StructField("hashtag", StringType, true),
      StructField("count", LongType, true)))

    val df = spark.createDataFrame(row_rdd, schema)
    println(s"---------------------------- \n ----- $time -----")

    df.createOrReplaceTempView("hashTags_table")
    val hashtags_count_df = spark.sql("select hashtag , count from hashTags_table order by count desc limit 10")
    hashtags_count_df.show()

    val arrayOfTopHashTags = hashtags_count_df.select("hashtag", "count").rdd.map(r => (r(0).toString(), r(1).toString().toLong)).collect

    arrayOfTopHashTags

  }

  def getConnection: Connection = {
    if (conn == null)
      conn = ConnectionFactory.createConnection(configHBase())

    conn
  }

  def configHBase() = {

    // simple config, using Dockerized HBase
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "localhost:2182")

    config

  }

     
}
