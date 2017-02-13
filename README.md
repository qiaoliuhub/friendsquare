# Friendsquare

## Real time friends suggestion

[www.friendsquare.info](https://www.friendsquare.info)

[Presentation](https://www.slideshare.net/secret/97d9OBMr9mYDxb)

Friendsquare is an application to suggest friend to users based on similar user behavior information. The website displays real-time updated suggested friends list for each user. This project is deployed on AWS platform and with following technologies:

- Apache Kafka
- Apache HDFS
- Spark
- Spark Streaming
- Apache Cassandra
- Flask

## Friendsquare pipeline

This pipeline can provide each user an interface to get friends suggestion based on historical and real-time updating users behaviors. Suggested friends are those who have most common venues visits with the querying user.

<p align="center">
  <img src="/images/pipeline.png" width="900"/>
</p>

## Data Generation and Ingestion

Streaming data is simulated with Kafka producer based on Foursquare user pool and venue pool in [foursquare_dataset 04/2012](https://archive.org/details/201309_foursquare_dataset_umn). Historical data was consumed with HDFS, blocked into 20MB and cached on HDFS. Streaming data was consumed with Spark_streaming with rate 300/s. This rate is set to be higher than average rate in archived foursquare_dataset 04/2012 (1.6/s) to stress streaming processing. Json format is like: {"userid": 1674265, "venueid": 679489, "rating": 2, "created_at": "2017-02-14 08:17:30"}

## Batch processing

Batch processsing has two steps: 
1.  Store each checkin information into 'checkins' table in Cassandra. 
2.  Count how many common venues visits for each pair of users and store user_user_count information in Cassandra. 
Batch processed results were directly written into cassandra with the spark-cassandra connector.

sbt libarary dependencies:
- "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
- "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
- "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3"

## Streaming processing

In streaming process, when a new checkin {"userid":"1674265", "venueid": 679489 ...} comes in, the following processes will be performed:
1.  Find out the user list who have visited the same venue ("venueid": 679489) before.
2.  Combine this userid with each userid in user list found in step 1 and construct user_user table A.
3.  For each user_user pair in table A (step 2), update number of common venues visits they have by adding 1 to the previous count.

sbt libarary dependencies:
- "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
- "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
- "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3"
- "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided"
- "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0"
- "com.github.benfradet" %% "spark-kafka-0-10-writer" % "0.2.0"

## Cassandra schema

Tables:
1.  checkins: table storing histrical and new user checkin activities
2.  user_friend and user_count: table storing number of common venues visits each user_user pair have
```
CREATE TABLE checkins (userid int, venueid int, PRIMARY KEY(venueid, userid));
--use userid, friendid to find out number of common venues visits they have
CREATE TABLE user_friend (userid int, count int, friendid int, PRIMARY KEY(userid, friendid));
--use userid to find out top 3 friendids who have most common venues visits with userid
CREATE TABLE user_count (userid int, count int, friendid int, flag boolean, PRIMARY KEY(userid, count, friendid)) WITH CLUSTERING ORDER BY (count DESC);
```
