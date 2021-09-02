package org.example

import org.apache.spark.sql._
import org.apache.spark.sql.types._

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

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("joob").master("local[*]").getOrCreate()

    val rawData = importMovies(spark)
    println("imported data")
    val cleanData = App.cleanData(rawData,spark)
    println("cleaned data")
    spark.stop()
  }

  // Query 1: find /tables/movie --q {"$where":{"$like":{"_id":"b%"}}}
  // Query 2: find /tables/movie --q {"$where":{"$and":[{"$eq":{"year":2010}},{"$like":{"title":"C%"}}]}}
}
