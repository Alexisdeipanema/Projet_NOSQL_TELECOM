import java.io.{File, FilenameFilter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode
import CsvImporter.{CsvNumber, CsvString, CsvType}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._

object Implementation_tables_requetes_un_mois {


  def main(args: Array[String]) {

    val conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.auth.username", "cassandra").set("spark.cassandra.auth.password", "cassandra")


    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .config(conf)
      .getOrCreate;

    spark.sparkContext.setLogLevel("ERROR")

    val colnames = "GLOBALEVENTID;SQLDATE;MonthYear;Year;FractionDate;Actor1Code;Actor1Name;Actor1CountryCode;Actor1KnownGroupCode;Actor1EthnicCode;Actor1Religion1Code;Actor1Religion2Code;Actor1Type1Code;Actor1Type2Code;Actor1Type3Code;Actor2Code;Actor2Name;Actor2CountryCode;Actor2KnownGroupCode;Actor2EthnicCode;Actor2Religion1Code;Actor2Religion2Code;Actor2Type1Code;Actor2Type2Code;Actor2Type3Code;IsRootEvent;EventCode;EventBaseCode;EventRootCode;QuadClass;GoldsteinScale;NumMentions;NumSources;NumArticles;AvgTone;Actor1Geo_Type;Actor1Geo_FullName;Actor1Geo_CountryCode;Actor1Geo_ADM1Code;Actor1Geo_ADM2Code;Actor1Geo_Lat;Actor1Geo_Long;Actor1Geo_FeatureID;Actor2Geo_Type;Actor2Geo_FullName;Actor2Geo_CountryCode;Actor2Geo_ADM1Code;Actor2Geo_ADM2Code;Actor2Geo_Lat;Actor2Geo_Long;Actor2Geo_FeatureID;ActionGeo_Type;ActionGeo_FullName;ActionGeo_CountryCode;ActionGeo_ADM1Code;ActionGeo_ADM2Code;ActionGeo_Lat;ActionGeo_Long;ActionGeo_FeatureID;DATEADDED;SOURCEURL".split(
      ";")

    val cols_to_keep = Array("GLOBALEVENTID",
      "SQLDATE",
      "MonthYear",
      "EventRootCode",
      "NumMentions",
      "AvgTone",
      "ActionGeo_CountryCode",
      "SOURCEURL")

   /* val parent =  new File("/home/alexis/IDEA/gdelt/src/main/Stockage_data_annuel")


    val mentions_filter = new FilenameFilter() {
      def accept(dir: File, name: String): Boolean = {
        if (name.contains("export") && name.contains("201803")){
          return true
        }
        else {
          return false
        }
      }
    }


    val liste = parent.list(mentions_filter)

    println(liste)

    for(name <- liste){


   try {


     val df_events_read_csv = spark.read
       .format("csv")
       .option("header", "false") //reading the headers
       .option("mode", "DROPMALFORMED")
       .option("delimiter", "\t")

       .load("/home/alexis/IDEA/gdelt/src/main/Stockage_data_annuel/" + s"$name").toDF(colnames: _*)

     val df_events = df_events_read_csv.select(col("globaleventid"),
       col("sqldate"),
       col("monthyear"),
       col("eventrootcode"),
       col("nummentions"),
       col("avgtone"),
       col("actiongeo_countrycode"),
       col("sourceurl"),
       col("Actor1Name").as("actor1name"),
       col("Actor2Name").as("actor2name")).filter(col("actiongeo_countrycode").isNotNull)
     //val df2= df.toDF(gdeltSchema: _*)

     //df1.take(2).foreach(println)


     df_events.write.format("org.apache.spark.sql.cassandra")
       .options(Map("keyspace" -> "nosql", "table" -> "events"))
       .mode(SaveMode.Append)
       .save()
   }
      catch {
        case _: Throwable => println("petite erreur mais on enchaine")
      }


}




     println("df_events_sauvegarde")
*//*
    val colnames_mentions = Array("globaleventid", "EventTimeDate", "MentionTimeDate", "MentionType", "MentionSourceName", "mentionidentifier", "SentenceID", "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset", "InRawText", "Confidence", "MentionDocLen", "MentionDocTone", "MentionDocTranslationInfo", "Extras")
    val cols_to_keep_mentions = Array("GLOBALEVENTID",
      "MentionIdentifier",
      "Confidence", "MentionDocTranslationInfo"
    )


    val parent2 =  new File("/home/alexis/IDEA/gdelt/src/main/Stockage_data_annuel/")


    val mentions_filter2 = new FilenameFilter() {
      def accept(dir: File, name: String): Boolean = {
        if (name.contains("mentions") && name.contains("201803")){
          return true
        }
        else {
          return false
        }
      }
    }


    val liste2 = parent2.list(mentions_filter2)

    for(name <- liste2){

      try {

      val df_mentions_read_csv = spark.read
        .format("csv")
        .option("header", "false") //reading the headers
        .option("mode", "DROPMALFORMED")
        .option("delimiter", "\t")
        .load("/home/alexis/IDEA/gdelt/src/main/Stockage_data_annuel/"+s"$name").toDF(colnames_mentions: _*)


      val df_mentions = df_mentions_read_csv
        .na.fill(Map(
        "MentionDocTranslationInfo" ->"EN"))
        .select(col("globaleventid"),
          col("MentionDocTone").as("mentiontone"),
          col("mentionidentifier"),
          substring(col("MentionDocTranslationInfo"), 7, 3).as("mentionlanguage"))




      df_mentions.write.format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace"->"nosql","table"->"mentions"))
        .mode(SaveMode.Append)
        .save()

    }

      catch {
        case _: Throwable => println("petite erreur mais on enchaine")

  }
}

*/

  println("mentions sauvegarde")

    val df_mentions= spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "mentions", "keyspace" -> "nosql"))
      .load()

    val df_events= spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "events", "keyspace" -> "nosql"))
      .load()

    val resultDf = df_mentions.join(df_events, Seq("globaleventid"))



    val df_rq5 = resultDf.na.fill(Map(
      "actor1name" ->"Unknown",
      "actor2name" ->"Unknown")).select(col("monthyear"),
      col("actor1name"),
      col("actor2name"),
      col("mentiontone"),
      col("globaleventid"),
      col("mentionidentifier"))

    df_rq5.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"nosql","table"->"rq5"))
      .mode(SaveMode.Append)
      .save()



/*
    resultDf.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"nosql","table"->"mentionsbyevent"))
      .mode(SaveMode.Append)
      .save()
*/
/*
   val df_rq1_bis = resultDf.select(col("sqldate"),
     col("actiongeo_countrycode"),
     col("mentionlanguage"),
     col("globaleventid"),
     col("mentionidentifier"))

    df_rq1_bis.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"nosql","table"->"rq1_bis"))
      .mode(SaveMode.Append)
      .save()

    println("rq1_sauvegarde")


    val df_rq2 = resultDf.na.fill(Map(
      "actor1name" ->"Unknown")).select(col("actor1name").as("actorname"),
      col("sqldate"),
      col("globaleventid"))


    df_rq2.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"nosql","table"->"rq2"))
      .mode(SaveMode.Append)
      .save()

    println("rq2_sauvegarde")

    val df_rq3_bis = resultDf.na.fill(Map(
      "actor1name" ->"Unknown", "mentiontone" -> 0)).select(col("monthyear"),
      col("actor1name").as("actorname"),
      col("actiongeo_countrycode"),
      col("mentionlanguage"),
      col("mentionidentifier"),
      col("globaleventid"),
      col("mentiontone"))

    df_rq3_bis.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"nosql","table"->"rq3_bis"))
      .mode(SaveMode.Append)
      .save()

    println("rq3_sauvegarde")

    val df_rq4_bis = resultDf.na.fill(Map(
      "actor1name" ->"Unknown","mentiontone" -> 0)).select(col("actor1name").as("actorname"),
      col("mentionidentifier"),
      col("mentiontone"),
      col("globaleventid"))


    df_rq4_bis.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"nosql","table"->"rq4"))
      .mode(SaveMode.Append)
      .save()

    println("Done")

    /* val xx=resultDf.groupBy("globaleventid").count().sort(desc("count"))
     println(xx.where("count >0").count())
     xx.join(dfmentions1,Seq("globaleventid")).show(5)
     /*resultDf.write.format("org.apache.spark.sql.cassandra")
       .options(Map("keyspace"->"nosql","table"->"mentionsbyevent2"))
       .mode(SaveMode.Append)
       .save()
   */
     println(df1.where(" 'actiongeo_countrycode' is not null").count())
     println(df1.count())OP
     println(dfmentions1.count())

   /*  val request1=resultDf.groupBy( "sqldate","actiongeo_countrycode","language").count()
     println(resultDf.count())

     //request1.show(5)
     resultDf.show(5)
     */

 */
 * */

  }

}



/*
* CREATE TABLE nosql.mentions     (       year int,       month int,       day int,
    * EventRootCode int,       NumMentions int,       AvgTone TEXT,       ActionGeo_CountryCode TEXT, ActionGeo_CountryCode TEXT,
       * SOURCEURL TEXT,       PRIMARY KEY ((SQLDATE, EventRootCode), ActionGeo_CountryCode, GLOBALEVENTID)     )
        * WITH CLUSTERING ORDER BY (ActionGeo_CountryCode ASC);


CREATE TABLE nosql.mentions
(
  GLOBALEVENTID int,
  sqldate TEXT,
  MentionIdentifier TEXT,
  Confidence int,
  AvgTone TEXT,
  ActionGeo_CountryCode TEXT,
  Language TEXT,
  PRIMARY KEY ((sqldate, ActionGeo_CountryCode, Language), GLOBALEVENTID, MentionIdentifier)
);


CREATE TABLE nosql.events (
    sqldate int,
    eventrootcode int,
    actiongeo_countrycode text,
    globaleventid int,
    avgtone text,
    monthyear int,
    nummentions int,
    sourceurl text,
    actor1name text,
    actor2name text,
    PRIMARY KEY ((monthyear,sqldate), actiongeo_countrycode, globaleventid)
) WITH CLUSTERING ORDER BY (actiongeo_countrycode ASC, globaleventid ASC)


CREATE TABLE nosql.mentions2 (
    globaleventid int,
    mentionidentifier text,
    language text,
    PRIMARY KEY (globaleventid, mentionidentifier)
) WITH CLUSTERING ORDER BY (mentionidentifier ASC)



CREATE TABLE nosql.mentions2 (
    globaleventid int,
    mentionidentifier text,
    language text,
    tone float,
    PRIMARY KEY (globaleventid, mentionidentifier)
) WITH CLUSTERING ORDER BY (mentionidentifier ASC)



CREATE TABLE nosql.mentions2 (
    globaleventid int,
    mentionidentifier text,
    language text,
    tone float,
    PRIMARY KEY (globaleventid, mentionidentifier)
) WITH CLUSTERING ORDER BY (mentionidentifier ASC)





CREATE TABLE nosql.mentionsbyevent (
    sqldate int,
    eventrootcode int,
    actiongeo_countrycode text,
    globaleventid int,
    avgtone text,
    monthyear int,
    nummentions int,
    sourceurl text,
    actor1name text,
    actor2name text,
    mentionidentifier text,
    mentionlanguage text,
    mentiontone float,

    PRIMARY KEY ((monthyear,sqldate,actiongeo_countrycode), mentionlanguage, globaleventid, mentionidentifier)
) WITH CLUSTERING ORDER BY (actiongeo_countrycode ASC, globaleventid ASC)



 CREATE TABLE nosql.mentionsbyevent (     sqldate int,     eventrootcode int,     actiongeo_countrycode text,     globaleventid int,     avgtone text,     monthyear int,     nummentions int,     sourceurl text,     actor1name text,     actor2name text,     mentionidentifier text,     mentionlanguage text,     mentiontone float,      PRIMARY KEY ((monthyear,sqldate,actiongeo_countrycode), mentionlanguage, globaleventid, mentionidentifier) ) WITH CLUSTERING ORDER BY (mentionlanguage ASC, globaleventid ASC)  ;

 CREATE TABLE nosql.mentionsbyevent2 (     sqldate int,     eventrootcode int,     actiongeo_countrycode text,     globaleventid int,     avgtone text,     monthyear int,     nummentions int,     sourceurl text,     actor1name text,     actor2name text,     mentionidentifier text,     mentionlanguage text,     mentiontone float,      PRIMARY KEY ((monthyear,sqldate) , actiongeo_countrycode, mentionlanguage, globaleventid, mentionidentifier) ) WITH CLUSTERING ORDER BY (actiongeo_countrycode ASC, mentionlanguage ASC, globaleventid ASC)  ;


*
*
* */