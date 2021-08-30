import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.App._
import org.example.Movie
import org.scalatest.FlatSpec

import scala.language.postfixOps
class AppTest extends FlatSpec{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkSession for unit tests")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
  import spark.implicits._

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  val movieWithIdSchema: StructType = StructType(Array(
    StructField("imdb_title_id", StringType, nullable = true),
    StructField("title", StringType, nullable = true),
    StructField("original_title", StringType, nullable = true),
    StructField("year", IntegerType, nullable = true),
    StructField("date_published", StringType, nullable = true),
    StructField("genre", StringType, nullable = true),
    StructField("duration", IntegerType, nullable = true),
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
    StructField("reviews_from_critics", DoubleType, nullable = true),
    StructField("country_id", StringType, nullable = true)
  ))


  "replaceUsingMap" should "replace a given string with its value from the supplied map converted to a string" in {
    val testString = "test"
    val testMap = Map[String,Int]("test" -> 1)
    val result = replaceUsingMap(testString,testMap)
    val expected = "1"
    assert(result == expected)
  }
  it should "replace occurences seperated by ', ' treating them as seperate keys" in {
    val testString = "test, test2"
    val testMap = Map[String,Int]("test" -> 1, "test2" -> 2)
    val result = replaceUsingMap(testString,testMap)
    val expected = "1, 2"
    assert(result == expected)
  }

  it should "replace unknown keys with \'UNKNOWN COUNTRY\'" in {
    val testString = "test2"
    val testMap = Map[String,Int]("test" -> 1)
    val result = replaceUsingMap(testString,testMap)
    val expected = "UNKNOWN COUNTRY"
    assert(result == expected)
  }

  it should "input a \"TYPEERROR\" string when encountering a key that is not a string" in {
    val testString = 2
    val testMap = Map[String,Int]("test" -> 1)
    val result = replaceUsingMap(testString,testMap)
    val expected = "TYPEERROR"
    assert(result == expected)
  }

  it should "input 59 when the given country is a null" in {
    val testMap = Map[String,Int]("test" -> 1)
    val result = replaceUsingMap(null,testMap)
    val expected = "59"

    assert(result == expected)
  }


  "CleanData" should "remove malformed rows and log a message bout their amount" in {
    val dataframeMalformed = spark.read.option("header", value = true).schema(movieSchema)
      .option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE")
      .csv(getClass.getResource("/mockMoviedata1Malformed.csv").toString)

    val dataframeCorrect = spark.read.option("header", value = true).schema(movieSchema)
      .option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE")
      .csv(getClass.getResource("/mockMoviedata1.csv").toString).drop("_corrupt_record")

    val dataframeCorrected = cleanData(dataframeMalformed,spark)
    val exceptResult: DataFrame = dataframeCorrected.except(dataframeCorrect).union(dataframeCorrect.except(dataframeCorrected))
    assert(exceptResult.count() == 0)
  }

  it should "have no effects on data with no malformed rows" in {
    val dataframeCorrect = spark.read.option("header", value = true).schema(movieSchema)
      .option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE")
      .csv(getClass.getResource("/mockMoviedata1.csv").toString).drop("_corrupt_record")

    val dataframeCorrected = cleanData(dataframeCorrect,spark)
    val exceptResult: DataFrame = dataframeCorrected.except(dataframeCorrect).union(dataframeCorrect.except(dataframeCorrected))
    assert(exceptResult.count() == 0)
  }

  "extractIndexedListFromColumn" should "extract an indexed list of the contents of a given dataframe column" in {
    val data = Seq("a", "b", "c", "d")
    val dataFrame = data.toDF("col1")
    val expected = List(("a",0),("b",1),("c",2),("d",3))

    val result: List[(String,Int)]=extractIndexedCountryList(dataFrame,"col1")
    assert(result == expected)
  }

  it should "contain only unique values from the dataframe" in {
    val data = Seq("a", "b", "b", "c", "d", "a")
    val dataFrame = data.toDF("col1")
    val expected = List(("a",0),("b",1),("c",2),("d",3))

    val result: List[(String,Int)]=extractIndexedCountryList(dataFrame,"col1")
    assert(result == expected)
  }

  it should "split the strings on ',' and treat the result pair as seperate elements" in {
    val data = Seq("a", "b, b", "c, d")
    val dataFrame = data.toDF("col1")
    val expected = List(("a",0),("b",1),("c",2),("d",3))

    val result: List[(String,Int)]=extractIndexedCountryList(dataFrame,"col1")
    assert(result == expected)
  }

  it should "trim whitespaces" in {
    val data = Seq("a ", "\tb", "c,    d")
    val dataFrame = data.toDF("col1")
    val expected = List(("a",0),("b",1),("c",2),("d",3))

    val result: List[(String,Int)]=extractIndexedCountryList(dataFrame,"col1")
    assert(result == expected)
  }

  it should "sort the elements in an alphabethical order before assiging indexes" in {
    val data = Seq("c", "d", "a", "b")
    val dataFrame = data.toDF("col1")
    val expected = List(("a",0),("b",1),("c",2),("d",3))

    val result: List[(String,Int)]=extractIndexedCountryList(dataFrame,"col1")
    assert(result == expected)
  }

  it should "filter out empty strings" in {
    val data = Seq("a,  ","b","c","d"," ")
    val dataFrame = data.toDF("col1")
    val expected = List(("a",0),("b",1),("c",2),("d",3))

    val result: List[(String,Int)]=extractIndexedCountryList(dataFrame,"col1")
    assert(result == expected)
  }

  it should "return an empty list if an empty df is given" in {
    val data: Seq[String] = Seq()
    val dataFrame = data.toDF("col1")
    val expected = List()

    val result: List[(String,Int)]=extractIndexedCountryList(dataFrame,"col1")
    assert(result == expected)
  }


  "createCountryIdDataFrameFromList" should "create a dataframe with '_id' and 'country' string-type columns from a (String,Int) List" in {
    val testList = List(("Pol",0),("Ger",1),("Aus",2))
    val expected = Seq(("0","Pol"),("1","Ger"),("2","Aus")).toDF("_id","country")

    val result = createCountryIdDataFrameFromList(testList,spark)

    val exceptResult: DataFrame = result.except(expected).union(expected.except(result))
    assert(exceptResult.count() == 0)
    assert(result.schema("_id").dataType.typeName == "string")
  }


  "enrichCountryToId" should "replace the country column with a country_id column using and udf made from replaceUsingMap and save the result to a DataSet[Movie]" in {
    val data = spark.read.option("header", value = true).schema(movieSchema)
      .option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE")
      .csv(getClass.getResource("/mockMoviedata1.csv").toString).drop("_corrupt_record")
    val list = List[(String,Int)](("Denmark",0),("Germany",1),("Italy",2),("USA",3))

    val result = enrichCountryToId(data,list,spark)
    val expected = spark.read
      .option("quote", "\"").option("escape", "\"").schema(movieWithIdSchema).option("header","true")
      .csv(getClass.getResource("/enrichedMoviedataExpected.csv").toString).as[Movie]

    val exceptResult = result.except(expected).union(expected.except(result))
    assert(exceptResult.count() == 0)

  }

  it should "fill in 59(France) when country is null" in {
    val data = spark.read.option("header", value = true).schema(movieSchema)
      .option("quote", "\"").option("escape", "\"").option("mode", "PERMISSIVE")
      .csv(getClass.getResource("/NoCountry.csv").toString)
    val list = List[(String,Int)](("Denmark",0),("Germany",1),("Italy",2),("USA",3))

    val result = enrichCountryToId(data,list,spark)
    assert(result.select("country_id").as[String].collect.head == "59")


  }
}
