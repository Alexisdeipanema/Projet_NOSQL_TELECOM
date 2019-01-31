import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.net.URL

import sys.process._
import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.ZipInputStream

object Downloader_and_dezipper_total {


  def main(args: Array[String]) {


    val conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.auth.username", "cassandra").set("spark.cassandra.auth.password", "cassandra")

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .config(conf)
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .format("csv")
      .option("header", "false") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", " ")
      .load("/home/alexis/IDEA/gdelt/src/main/resources/master2.txt").toDF("0", "1", "2")

    //df.show(5)

    val df1 = df.select(col("2"))

    df1.foreach { row =>
      row.toSeq.foreach { col =>
        if (col != null && !col.toString.contains("gkg")) {


          println(col.toString())

          try {
            new URL(col.toString()) #> new File("/home/alexis/IDEA/gdelt/src/main/Stockage_data_annuel/output_test_zip") !!

            println("ok")
            val fis = new FileInputStream("/home/alexis/IDEA/gdelt/src/main/Stockage_data_annuel/output_test_zip")
            val zis = new ZipInputStream(fis)

            Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
              val fout = new FileOutputStream("/home/alexis/IDEA/gdelt/src/main/Stockage_data_annuel/" + file.getName)
              val buffer = new Array[Byte](1024)
              Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(fout.write(buffer, 0, _))

            }
          }

          catch {
            case _: Throwable => println("adresse url invalide")
          }

        }
        //println(col) }
      }
    }

  }

}

    /*

    df1.foreach { row =>
      row.toSeq.foreach{
        //col2 =>
        //println(row.toString())
        // val url:URL=new URL(row.toString())
        val tt = new URL(row.toSeq[0].asInstanceOf[String]) #> new File("Output.csv") !!
        // val result = scala.io.Source.fromURL(url).mkString
        //only one line inputs are accepted. (I tested it with a complex Json and it worked)
        //val jsonResponseOneLine = result.toString().stripLineEnd
        //println(jsonResponseOneLine) }
      }
      */

      /*
        val df = spark.read
          .format("csv")
          .option("header", "false") //reading the headers
          .option("mode", "DROPMALFORMED")
          .option("delimiter","\t")
          .load("file:///Users/guillaumequispe/dev/Intellij/gdelt/src/main/resources/20150218224500.translation.export.CSV").toDF(colnames:_*)


  import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

  def GetUrlContentJson(url: String): DataFrame ={
    val result = scala.io.Source.fromURL(url).mkString
    //only one line inputs are accepted. (I tested it with a complex Json and it worked)
    val jsonResponseOneLine = result.toString().stripLineEnd
    //You need an RDD to read it with spark.read.json! This took me some time. However it seems obvious now
    val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil)

    val jsonDf = spark.read.json(jsonRdd)
    return jsonDf
  }
  val response = GetUrlContentJson(url)
  response.show
*/
