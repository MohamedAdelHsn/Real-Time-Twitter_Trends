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

import scala.util.Try
import org.slf4j.LoggerFactory



object myDEmo  {
  
  @transient lazy val logger=LoggerFactory.getLogger(this.getClass())
  
  def main(args: Array[String]): Unit = {
    
    
      val twProperties = Utils.getTwitterProperties();
      
      System.setProperty("twitter4j.oauth.consumerKey", twProperties.getProperty("CONSUMER_KEY"))
      System.setProperty("twitter4j.oauth.consumerSecret", twProperties.getProperty("CONSUMER_SECRET"))
      System.setProperty("twitter4j.oauth.accessToken", twProperties.getProperty("ACCESS_TOKEN"))
      System.setProperty("twitter4j.oauth.accessTokenSecret", twProperties.getProperty("ACCESS_TOKEN_SECRET"))
     

      val sparkConf = new SparkConf()
      .setAppName("realtime-tracking-popular-hashTags")
      .setMaster("local[3]")    
      /*.setJars(Array("/home/hdpadmin/Desktop/Jars/spark-streaming-twitter_2.12-2.4.0.jar" 
          , "/home/hdpadmin/Desktop/Jars/twitter4j-core-4.0.7.jar" 
          , "/home/hdpadmin/Downloads/twitter4j-stream-4.0.7.jar"))
     */
      
      val streamingContext = new StreamingContext(sparkConf, Seconds(1));
     
      streamingContext.sparkContext.setLogLevel("OFF");
            
      val tweets = TwitterUtils.createStream(streamingContext , None)
      
      val statuses = tweets.map(status => status.getText)
      
      val hashTags = statuses.flatMap(_.split(" ")).filter(_.startsWith("#"))
      
      val hashTagKeyValue = hashTags.map((_ , 1))     
      
      val hashTagsCounts:DStream[(String, Long)] = hashTagKeyValue
      .updateStateByKey(aggregate_hashtags_count)
     // .reduceByKeyAndWindow((x:Int,y:Int)=>(x+y), (x:Int,y:Int)=>(x-y), Seconds(15), Seconds(3))
      
      //.transform(rdd => rdd.sortBy(x => x._2 , false))
      
      
      
      val schema = StructType( Array(
                 StructField("hashtag", StringType,true),
                 StructField("count", LongType,true)
             ))
      
       
      // transform rdds to df and clean data then post it via REST API 
      hashTagsCounts.foreachRDD(process_rdd( _,  _ , schema))
      hashTagsCounts.foreachRDD(rdd => if(!rdd.isEmpty()) rdd.foreach(send2Hbase(_)))
      
      
      // Writing dstream of hashtags counts to HBase (The Hadoop Database for realTime)      
      //hashTagsCounts.foreachRDD(rdd => if(!rdd.isEmpty()) rdd.foreach(send2Hbase(_)))
   
      //hashTagsCounts.print(10)
     
      streamingContext.checkpoint("/home/hdpadmin/workspace/mytwitterApp/src/main")
      streamingContext.start()
      streamingContext.awaitTermination()
    
    
    
  }
  
     //######################### My Custom Stateful Transformation Function ################################//
    
     def aggregate_hashtags_count(newData: Seq[Int], state: Option[Long]) = {
   
      val newState = state.getOrElse(0L) + newData.sum 
      Some(newState)
  
  }
    
  
     // #########################  Load processed data in low latency data store  ###############################//
    
     def send2Hbase(row:(String , Long)) :Unit = 
     {

       val table_name = "hashtags_trends"
       val configuration = HBaseConfiguration.create();
       configuration.set("hbase.zookeeper.quorum", "localhost:2182");
       val connection = ConnectionFactory.createConnection(configuration);
        //Specify the target table
       val table = connection.getTable(TableName.valueOf(table_name));
       val put = new Put(row._1.getBytes())
       
       put.addColumn("hashtags".getBytes(), "counts".getBytes(), row._2.toString().getBytes());
       table.put(put);
      
     
     }
       
     
    // ###########################  Visualize data in real-live Dashboard  ##################################//

         
     def process_rdd(rdd:RDD[(String , Long)] , time :Time , schema: StructType) {
          
         val spark = new SQLContext(rdd.context)        
         val row_rdd = rdd.map(record => Row(record._1 , record._2))
         
         
         val df = spark.createDataFrame(row_rdd , schema)
         println(s"---------------------------- \n ---- $time ----")
         
         df.createOrReplaceTempView("hashTags_table")        
         val hashtags_count_df = spark.sql("select hashtag , count from hashTags_table order by count desc limit 10")
         hashtags_count_df.show()
                  
         
         val top20HashTags:List[String] =  hashtags_count_df.select("hashtag").rdd.map(r => r(0).toString()).collect().toList        
         val topHashTagsCount :List[String] = hashtags_count_df.select("count").rdd.map(r => r(0).toString()).collect().toList
         
         
         Utils.post_to_flaskDashboard("http://127.0.0.1:5000/updateData" , top20HashTags ,topHashTagsCount)
       
  }

     
}
