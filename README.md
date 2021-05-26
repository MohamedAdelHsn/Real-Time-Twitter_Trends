# Real-Time-Twitter_Trends
Here's a real-time dashboard that say What happens around the world every second ? by Tracking top twitter trends and hashtags using Big data Technology <br />  <br />

https://user-images.githubusercontent.com/58120325/119686926-13dae480-be47-11eb-910a-020197c5ac83.mp4


1- Twitter acts as a Producer of stream of data and we should use api to access tweets from twitter here's we use #twitter4J

2- Spark Streaming(In-Memory real time distributed Computing Engine )
that will consume tweets when they arrived and represent them as batches to apply stateful transformation to update the old data with new information for each hashtag (count hashtags)

3- After applying transformation we will post processed batches to flask server to visualize data in real-time dashboard

4- apache HBase acts as storage layer built on top of Hadoop File System with low latency access (read / Write ) because writing stream data to HDFS is a high cost operation

