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
import org.apache.log4j.{Logger , Level}
import scala.util.Try
import org.slf4j.LoggerFactory


object TwitterTrends  {
  
  def main(args: Array[String]): Unit = {
        
      val twProperties = Utils.getTwitterProperties();
      
      System.setProperty("twitter4j.oauth.consumerKey", twProperties.getProperty("CONSUMER_KEY"))
      System.setProperty("twitter4j.oauth.consumerSecret", twProperties.getProperty("CONSUMER_SECRET"))
      System.setProperty("twitter4j.oauth.accessToken", twProperties.getProperty("ACCESS_TOKEN"))
      System.setProperty("twitter4j.oauth.accessTokenSecret", twProperties.getProperty("ACCESS_TOKEN_SECRET"))
     

      Logger.getLogger("org").setLevel(Level.ERROR);      
    
      val sparkConf = new SparkConf()
      .setAppName("realtime-tracking-popular-hashTags")
      .setMaster("local[2]")    

      
      val streamingContext = new StreamingContext(sparkConf, Seconds(1));    
      streamingContext.sparkContext.setLogLevel("OFF");
    
     //######################### Extract Data From twitter every 1 second ################################//
    
      val tweets = TwitterUtils.createStream(streamingContext , None)     
      val statuses = tweets.map(status => status.getText)
      val hashTags = statuses.flatMap(_.split(" ")).filter(_.startsWith("#"))
      val hashTagKeyValue = hashTags.map((_ , 1))     
    
      val hashTagsCounts:DStream[(String, Long)] = hashTagKeyValue
      .reduceByKey(_+_)
      .updateStateByKey(aggregate_hashtags_count)
    
     // .reduceByKeyAndWindow((x:Int,y:Int)=>(x+y), (x:Int,y:Int)=>(x-y), Seconds(15), Seconds(3))    
      //.transform(rdd => rdd.sortBy(x => x._2 , false))
      
    //---------------------------- i have  a new code updates for more optimization ---------------------------------- //
    
      // transform rdds to df and clean data then post it via REST API 
      hashTagsCounts.foreachRDD(process_rdd( _,  _ ))
    
      // store data streams to Hbase (The Hadoop Database for Real-time)
      hashTagsCounts.foreachRDD(rdd => if(!rdd.isEmpty()) rdd.foreach(send2Hbase(_)))
     
     
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
       
     
    // ###########################  Visualize data in real-live Dashboard (Rest API --Flask)  ##################################//

         
     def process_rdd(rdd:RDD[(String , Long)] , time :Time) {
          
         val spark = new SQLContext(rdd.context)        
         val row_rdd = rdd.map(record => Row(record._1 , record._2))
         
        // define schema to create dataframe
        val schema = StructType( Array(
                 StructField("hashtag", StringType,true),
                 StructField("count", LongType,true)
             ))
      
         
         val df = spark.createDataFrame(row_rdd , schema)
         println(s"---------------------------- \n ---- $time ----")
         
         df.createOrReplaceTempView("hashTags_table")        
         val hashtags_count_df = spark.sql("select hashtag , count from hashTags_table order by count desc limit 10")
         hashtags_count_df.show()
                  
         
         val top10HashTags:List[String] =  hashtags_count_df.select("hashtag").rdd.map(r => r(0).toString()).collect().toList        
         val top10HashTagsCount :List[String] = hashtags_count_df.select("count").rdd.map(r => r(0).toString()).collect().toList
         
         
         Utils.post_to_flaskDashboard("http://127.0.0.1:5000/updateData" , top10HashTags ,top10HashTagsCount)
       
  }

     
}
