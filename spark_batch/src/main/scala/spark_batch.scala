import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import scala.collection.JavaConversions._
import org.apache.spark.sql.{Row,SaveMode}
import org.apache.spark.sql.DataFrame
import com.datastax.spark.connector._

// Input historical data from hdfs and output joined and aggregated data to CassandraDB
    
object spark_batch {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Friendsquare").config("spark.cassandra.connection.host", args(1)).getOrCreate()    
    import spark.implicits._

    //Read information from hdfs and store as DataFrame
    val jsonDF = spark.read.json("hdfs://"+args(0)+":9000/user/Friendsquare/*")
    
    val venue_userDF = jsonDF.select($"venueid", $"userid").distinct.persist
    
    //write DataFrame to cassandra playground.checkins
    venue_userDF.write.format("org.apache.spark.sql.cassandra").options(Map("table"->"checkins","keyspace"->"playground")).mode(SaveMode.Append).save()
    
    // self join
    val user_user_RDD=venue_userDF.as("df1").join(venue_userDF.as("df2"), $"df1.venueid" === $"df2.venueid")
                                   .select($"df1.userid", $"df2.userid", $"df1.venueid").rdd

    //Caculate number of same venues between each two user pair
    val user_user_unique=user_user_RDD.distinct.map(record=>((record(0).toString, record(1).toString), 1))
                                        .reduceByKey(_+_)
                                         .map(record => common_visit(record._1._1.toInt, record._1._2.toInt, record._2))
                                          .filter(_.isValid)
                                           .toDF()
    
    //user_user_unique.show()

    //Save batch processing results to CassandraDB
    user_user_unique.write.format("org.apache.spark.sql.cassandra").options(Map("table"->"user_friend","keyspace"->"playground")).mode(SaveMode.Append).save()
        
    user_user_unique.map(record=>sorted_common_visit(record.getAs[Int]("userid"), record.getAs[Int]("friendid"), record.getAs[Int]("count"), 1)).write.format("org.apache.spark.sql.cassandra").options(Map("table"->"user_count","keyspace"->"playground")).mode(SaveMode.Append).save()
    
    spark.stop()
    }
}

case class common_visit(userid: Int, friendid: Int, count: Int){
    def isValid = {userid!=friendid}
}

case class sorted_common_visit (userid: Int, friendid: Int, count: Int, flag: Int)
