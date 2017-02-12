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
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import java.util.Properties
import com.github.benfradet.spark.kafka010.writer._
import org.apache.kafka.common.serialization.StringSerializer



object spark_kafka {
  def main(args: Array[String]) {

    val brokers = args(0)+":9092"
    val topics = "Friendsquare"
    val topicsSet = topics.split(",").toSet

    // Create context with 5 second batch interval
    val sparkConf = new SparkConf().setAppName("Friendsquare").set("spark.cassandra.connection.host",args(1))
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val json_messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    
    //Create kafkaproducer property
    val producerConfig = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", (args(0)+":9092"))
    p.setProperty("key.serializer", classOf[StringSerializer].getName)
    p.setProperty("value.serializer", classOf[StringSerializer].getName)
    p
    }
    
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
      
      incommonRDD.collect().foreach(println)
      //Produce data to different topics
      incommonRDD.map(message=>message.toString).writeToKafka(producerConfig, 
                                                              s => new ProducerRecord[String, String]("Streaming_1", s))
      
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
    
   override def toString(): String = ""+userid.toString + "," + count.toString + "," + friendid.toString;
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

