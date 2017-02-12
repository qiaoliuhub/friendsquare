import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.driver.core.utils._
import org.apache.spark.rdd._
import com.datastax.driver.core.{CodecRegistry, ResultSet, Row, TypeCodec}

object spark_streaming {
  def main(args: Array[String]) {

    val brokers = args(0)+":9092"
    val topics = "Friendsquare"
    val topicsSet = topics.split(",").toSet

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("Friendsquare").set("spark.cassandra.connection.host","172.31.0.204")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val json_messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    //Parse Json messages
    val messages = json_messages.map(_._2).map(
      parse(_).asInstanceOf[JObject]
    ).map(json => {
      implicit val formats = DefaultFormats
      val rating = (json \ "rating").extract[Int]
      val created_at = (json \ "created_at").extract[String]
      val partitionkey = (json \ "partitionkey").extract[String]
      val userid = (json \ "userid").extract[Int]
      val venueid = (json \ "venueid").extract[Int]
      Visit(rating, created_at, partitionkey, userid, venueid)
    }).filter(_.isValid)

    // Process each RDD and update CassandraDB
    messages.foreachRDD { rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      
      //Join rdd with playground.checkins table in CassandraDB based on column venueid
      rdd.saveToCassandra("playground","checkins", SomeColumns("venueid", "userid"))
      val incommonRDD = rdd.joinWithCassandraTable("playground","checkins").on(SomeColumns("venueid"))
                            .map(record=> Noincommon(record._1.userid, 1, record._2.get[Int]("userid"))
                                ).filter(_.isValid).persist
        
      //Retrieve information that is needed to be updated from playground.user_friend
      val updateRDD = incommonRDD.joinWithCassandraTable("playground","user_friend").on(SomeColumns("userid", "friendid"))
                                  .map(record=> {Noincommon(record._1.userid, record._2.get[Int]("count"), record._1.friendid)
                                            }).persist
      
      //This information is needed to be removed from playground.user_count
      val removedRDD = updateRDD.map(record=>Noincommon (record.friendid, record.count, record.userid)) ++ updateRDD
      removedRDD.map(record=> flag(record.userid, record.count, record.friendid, 0)).saveToCassandra("playground","user_count", SomeColumns("userid", "count", "friendid", "flag"))
      
      //Updated information that is ready to write to CassandraDB
      val mergeRDD = (incommonRDD ++ updateRDD).map(record => ((record.userid, record.friendid),record.count)).reduceByKey(_+_)
                                                .map(record => Noincommon(record._1._1, record._2, record._1._2))
        
      val undirectedRDD = (mergeRDD.map(record=>Noincommon (record.friendid, record.count, record.userid)) ++ mergeRDD).persist
      
      undirectedRDD.saveToCassandra("playground","user_friend", SomeColumns("userid", "count", "friendid"))
      
      undirectedRDD.map(record=> flag(record.userid, record.count, record.friendid, 1)).saveToCassandra("playground","user_count", SomeColumns("userid", "count", "friendid", "flag"))
      
      undirectedRDD.collect().foreach(println)

    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

case class Visit(rating: Int, created_at: String, partitionkey: String, userid: Int, venueid: Int){
  def isValid = {
    created_at!="" && partitionkey != "" && userid != 0 && venueid != 0
  }
}

case class Noincommon (userid: Int, count: Int, friendid: Int){
   def isValid = {userid!=friendid}
}

case class flag(userid: Int, count: Int, friendid: Int, flag: Int)
/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
/* Created by QiaoLiu1 on 1/31/17.*/

