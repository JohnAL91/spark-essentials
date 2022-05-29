package part3typesdatasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common spark types")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47) as "plain_value")

  // Booleans
  val dramaFilter: Column = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter: Column = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter)
  val moviesWithGoodnessFlagsDF = moviesDF.select($"Title", preferredFilter.as("good_movie"))

  // filter on boolean column
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === true

  // negations
  moviesWithGoodnessFlagsDF.where(not($"good_movie"))

  // Numbers
  // math operators
  val moviesAvgRatingsDF = moviesDF.select($"Title", ($"Rotten_Tomatoes_Rating" / 10 + $"IMDB_Rating") / 2)
  moviesAvgRatingsDF.show()

  // Correlation = number between -1 and 1 (pearson correlation)
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // Corr is an action

  // Strings
  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap($"Name")).show()

  //contains
  carsDF.select("*").filter($"Name".contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    $"Name",
    regexp_extract($"Name", regexString, 0).as("regex_extract")
  ).where($"regex_extract" =!= "")

  vwDF.select(
    $"Name",
    regexp_replace($"Name", regexString, "People's Car").as("regex_replace")
  ).show()

  /** Exercise
    *
    *  Filter the cars dataframe with a list of a car names obtained by an API call
    * */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // Version 1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase).mkString("|")
  carsDF.select(
    $"Name",
    regexp_extract(lower($"Name"), complexRegex, 0).as("regexComplex")
  )
    .filter($"regexComplex" =!= "")
    .show()

  // Version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase).map($"Name".contains(_))
  val bigFilter = carNameFilters.fold(lit(false))((x, y) => x or y)
  carsDF.filter(bigFilter).show()

  // My version... :)
  carsDF.select($"Name".isin(getCarNames:_*)).show()
}
