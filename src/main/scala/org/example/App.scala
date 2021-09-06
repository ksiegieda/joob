package org.example

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{collect_set, concat_ws, explode, first, udf}
import org.apache.spark.sql.types._

import java.util.UUID

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

  def importMovies(spark:SparkSession): Dataset[Movie] = {
    import spark.implicits._
    spark.read
      .option("header", value = true)
      .schema(movieSchema)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("mode", "PERMISSIVE")
//      .csv("hdfs:/user/mapr/data/IMDb movies.csv")
      .csv("file:///C:/SII/LAT/joob/data/IMDb-movies.csv")
      .as[Movie]
  }

  def cleanData(ds:Dataset[Movie], spark:SparkSession): Dataset[Movie] = {
    import spark.implicits._
    val malformedRows = ds.filter($"_corrupt_record".isNotNull)
    malformedRows.cache()
    val numMalformedRows = malformedRows.count()
    if (numMalformedRows > 0) {
      println(s"found $numMalformedRows malformed record(s). Skipping them")
    }
    malformedRows.unpersist()
    ds.select("*").where($"_corrupt_record".isNull).drop($"_corrupt_record").as[Movie]
  }

  def explodeContries(ds:Dataset[Movie], spark:SparkSession): Dataset[Movie] = {
    import spark.implicits._
    val sds = ds.withColumn("country", functions.split($"country",", "))
      .withColumn("country",explode($"country"))
    sds.as[Movie]
  }

  def distinctCountries(ds:Dataset[Movie], spark:SparkSession): DataFrame = {
    ds.select("country").distinct()
  }

  def createCountryIDs(df:DataFrame, spark:SparkSession): DataFrame = {
    import spark.implicits._
    val generateUUID = udf((a:String) => UUID.nameUUIDFromBytes(a.getBytes).toString)
    val sdf = df.select("country").withColumn("_id",generateUUID($"country"))
    sdf
  }

  def joinData(imdb: Dataset[Movie], dict: Dataset[IDMovie], spark:SparkSession): Dataset[Movie] = {
    import spark.implicits._
    val result = imdb.join(dict,Seq("country"),"fullouter").drop("country")
    result.as[Movie]
  }

  def collectCountries(ds:Dataset[Movie],spark: SparkSession): Dataset[Movie] = {
    import spark.implicits._
    val columnMap: Array[Column] = ds.columns.filterNot(a => a == "_id")
      .map(a => first(a).as(a)):+ concat_ws(", ",collect_set("_id"))
      .as("country_id")
    ds.groupBy($"imdb_title_id".as("_id")).agg(columnMap.head, columnMap.tail: _*).as[Movie]
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("joob").master("local[*]").getOrCreate()
    import spark.implicits._

    val rawData = importMovies(spark)
    println("imported data")
    val cleanData = App.cleanData(rawData,spark)
    println("cleaned data")
    val explodedCountries = explodeContries(cleanData,spark)
    val countriesDF = distinctCountries(explodedCountries, spark)
    val countriesIDDF = createCountryIDs(countriesDF, spark)
//    countriesIDDF.saveToMapRDB("/tables/country")
    val countriesIDDS: Dataset[IDMovie] = countriesIDDF.as[IDMovie]
    val joined = joinData(explodedCountries, countriesIDDS, spark)
    val collected = collectCountries(joined, spark)
//    collected.saveToMapRDB("/tables/movie")


    spark.stop()

  }

  // Query 1: find /tables/movie --q {"$where":{"$like":{"_id":"b%"}}}
  // Query 2: find /tables/movie --q {"$where":{"$and":[{"$eq":{"year":2010}},{"$like":{"title":"C%"}}]}}
}
