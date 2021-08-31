package org.example

import com.mapr.db.spark.sql.toMapRDBDataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object App {
  val movieSchema: StructType = StructType(Array(
    StructField("imdb_title_id", StringType, nullable = true),
    StructField("title", StringType, nullable = true),
    StructField("original_title", StringType, nullable = true),
    StructField("year", IntegerType, nullable = true),
    StructField("date_published", StringType, nullable = true),
    StructField("genre", StringType, nullable = true),
    StructField("duration", IntegerType, nullable = true),
    StructField("country", StringType, nullable = true),
    StructField("language", StringType, nullable = true),
    StructField("director", StringType, nullable = true),
    StructField("writer", StringType, nullable = true),
    StructField("production_company", StringType, nullable = true),
    StructField("actors", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("avg_vote", DoubleType, nullable = true),
    StructField("votes", IntegerType, nullable = true),
    StructField("budget", StringType, nullable = true),
    StructField("usa_gross_income", StringType, nullable = true),
    StructField("worlwide_gross_income", StringType, nullable = true),
    StructField("metascore", DoubleType, nullable = true),
    StructField("reviews_from_users", DoubleType, nullable = true),
    StructField("reviews_from_critics", DoubleType, nullable = true)
  )).add("_corrupt_record", StringType, nullable = true)

  def replaceUsingMap(country:Any, givenMap: Map[String,Int]): String = country match {
    case str: String => try {
      str.split(", ").map(givenMap(_)).mkString(", ")
      //TODO tu wyjatek unkown country, czy da sie to zrobic optymalniej? i ogolnie o apply
      //TODO trima dac
    } catch {
      case _: NoSuchElementException => "UNKNOWN COUNTRY"
      case _: Exception =>
        println("Unexpected Exception, fillin in \"EXERROR\"")
        "EXERROR"
    }
    case _ if country == null => "59" //TODO givenMap("France").toString is better
    //TODO magic number
    case _ =>
      println("Unexpected type, filling in \"TYPEERROR\"")
      "TYPEERROR"
      //TODO zastanowic sie czy istnieje szansa, zeby to nie byl string ALBO country:Any -> country:String
    //TODO wyjatek gdy nie string moze, trzeba zmniejszyc odpowiedzialnosc funkcji
  }

  def importMovieData(spark:SparkSession): DataFrame = {
    spark.read.option("header", value = true).schema(movieSchema).option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE")
      .csv("hdfs:/user/mapr/data/IMDb movies.csv").cache()
  }

  def cleanData(df:DataFrame, spark:SparkSession): DataFrame = {
    import spark.implicits._
    val malformedRows = df.filter($"_corrupt_record".isNotNull)
    //TODO sprawdzic DAG
    malformedRows.cache()
    val numMalformedRows = malformedRows.count()
    if (numMalformedRows > 0) {
      println(s"found $numMalformedRows malformed record(s). Skipping them")
    }
    malformedRows.unpersist()
    //TODO cache i unpersist sprawdzic
    df.select("*").where($"_corrupt_record".isNull).drop($"_corrupt_record")
  }

  def extractIndexedCountryList(df:DataFrame,colName:String): List[(String,Int)] =
  {
    df.select(colName).as(Encoders.STRING).filter(_.nonEmpty).collect()
      //TODO zrobic to w sposob rozproszony, df zamiast list
      .flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty).distinct.sorted.zipWithIndex.toList
    //TODO distinct na liscie a na df
  }

  def createCountryIdDataFrameFromList(list: List[(String,Int)],spark: SparkSession): DataFrame = {
    import spark.implicits._
    //TODO parallelize, zmienic na df
    list.map( a => (a._2.toString,a._1)).toDF("_id", "country")
  }

  def enrichCountryToId(df:DataFrame, list: List[(String,Int)], spark: SparkSession): Dataset[Movie] = {
    import spark.implicits._
    val otherUdf = udf((a: Any) => replaceUsingMap(a,list.toMap))
    //TODO co jezeli lista jest bardzo duza i sie nie miesci w pamieci na 1 maszynie - zastanowic sie
    try {
      val updatedDataframe = df.withColumn("country_id", otherUdf(df("country"))).drop("country")
      updatedDataframe.as[Movie]
    } catch {
      case e:AnalysisException =>
        //TODO usunac
        println("An analysis exception has occurred  \n" + e.message)
        println("returning blank movie ds")
        Seq(Movie("title_id","title","original_Title",None,"date","genre",None,None,None,None,None,None,None,None,0.0,1,None,None,None,None,None,None)).toDS()
    }

  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("joob").master("local[*]").getOrCreate()
//TODO tu mastera sprawdzic czy sie, sprawdzic w yarnie czy to sie odpala i sprawdzic to i lokalnie, i u klienta

    val data = importMovieData(spark)
    println("imported data")
    val cleanData = App.cleanData(data,spark)
    println("cleaned data")
    val countryList = extractIndexedCountryList(cleanData,"country")
    println("extracted list")
    val countryDF = createCountryIdDataFrameFromList(countryList,spark)
    println("created country df")
    countryDF.saveToMapRDB("/tables/country")
    println("saved countries to maprdb")
    val enrichedMoviesDS = enrichCountryToId(cleanData,countryList,spark)
    println("enriched dataframe to dataset")
    enrichedMoviesDS.saveToMapRDB("/tables/movie", idFieldPath = "imdb_title_id")
    println("saved")
//TODO zobaczyc jak te dane sa partycjonowane po drodze
    //TODO ogolna uwagi: robic to na df lub ds bez transformowania niepotrzebnego, usunac collect/pozbyc sie
    spark.stop()
  }
  // Query 1: find /tables/movie --q {"$where":{"$like":{"_id":"b%"}}}
  // Query 2: find /tables/movie --q {"$where":{"$and":[{"$eq":{"year":2010}},{"$like":{"title":"C%"}}]}}
}
