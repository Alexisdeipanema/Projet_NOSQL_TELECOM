import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode
import CsvImporter.{CsvNumber, CsvString, CsvType}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.joda.time.DateTime


object Specific_queries {

  def main(args: Array[String]) {
    /* val sc = new SparkContext("local", "test", conf)
       val rdd = sc.cassandraTable("test", "kv")
       println(rdd.count)
       println(rdd.first)
       println(rdd.map(_.getInt("value")).sum)

       val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
       collection.saveToCassandra("nosql", "events", SomeColumns("key", "value"))
       */
    val conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.auth.username", "cassandra").set("spark.cassandra.auth.password", "cassandra")

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .config(conf)
      .getOrCreate;
    spark.sparkContext.setLogLevel("ERROR")

    val df_rq1 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "rq1", "keyspace" -> "nosql"))
      .load()

    val df_rq1_bis = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "rq1_bis", "keyspace" -> "nosql"))
      .load()

    val df_rq2 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "rq2", "keyspace" -> "nosql"))
      .load()

    val df_rq3 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "rq3", "keyspace" -> "nosql"))
      .load()

    val df_rq3_bis = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "rq3_bis", "keyspace" -> "nosql"))
      .load()


    val df_rq4 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "rq4", "keyspace" -> "nosql"))
      .load()

    val df_mentionsbyevent = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mentionsbyevent", "keyspace" -> "nosql"))
      .load()

    val df_rq5 = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "rq5", "keyspace" -> "nosql"))
      .load()


    //request5
    val t15 = System.nanoTime()
    val evolution = df_rq5.where("(actor1name = 'UNITED STATES') and (actor2name = 'CHINA')").groupBy("monthyear").agg(avg("mentiontone"))
    evolution.show(5)
    val t16 = System.nanoTime()
    val time_req5 = (t16 - t15)/1000000000
    println(s"temps d'exécution requête 5 : $time_req5")


    // request 1
    val t9 = System.nanoTime()
    val df_request1 = df_rq1_bis.groupBy("sqldate", "actiongeo_countrycode", "mentionlanguage","globaleventid").count()
    df_request1.show(5)
    val t10 = System.nanoTime()
    val time_req1 = (t10 - t9)/1000000000



    println(s"temps d'exécution requête 1 : $time_req1")






    /*

    //request2

    */
    val t3 = System.nanoTime()
    val date = DateTime.now()
    val format = "yyyyMMdd"
    val date_numerique = date.plusMonths(-6).toString(format).toInt

    println(date_numerique)

    df_rq2.where(s"(sqldate < $date_numerique) and (actorname = 'PRESIDENT')").select(col("globaleventid"), col("sqldate"), col("actorname")).show(5)
    val t4 = System.nanoTime()
    val time_req2 = (t4 - t3)/1000000000


    println(s"temps d'exécution requête 2 : $time_req2")



    //request3'




    val t11 = System.nanoTime()
    val Meilleurs_positifs_bis = df_rq3_bis.where("(mentiontone > 0)").groupBy( "monthyear","actiongeo_countrycode","mentionlanguage", "actorname").count().sort(desc("count"))
    Meilleurs_positifs_bis.show(5)

    val Meilleurs_negatifs_bis = df_rq3_bis.where("(mentiontone < 0)").groupBy( "monthyear","actiongeo_countrycode","mentionlanguage", "actorname").count().sort(desc("count"))
    Meilleurs_negatifs_bis.show(5)
    val t12 = System.nanoTime()
    val time_req3_bis = (t12 - t11)/1000000000
    println(s"temps d'exécution requête 3 : $time_req3_bis")

    //request4

    val t7 = System.nanoTime()
    val Contestation = df_rq4.groupBy("actorname").agg(stddev("mentiontone")).filter(!(isnan(col("stddev_samp(mentiontone)")))).sort(desc("stddev_samp(mentiontone)"))
    Contestation.show(5)
    val t8 = System.nanoTime()
    val time_req4 = (t8 - t7)/1000000000
    println(s"temps d'exécution requête 4 : $time_req4")




  }

}