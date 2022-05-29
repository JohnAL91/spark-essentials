package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF.select($"Title", to_date($"Release_Date", "dd-MMM-yy").as("Actual_Release"))
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff($"Today", $"Actual_Release") / 365) // Also have date_add and date_sub

  moviesWithReleaseDates.filter($"Actual_Release".isNull).show()

  /**
    * Exercise
    * 1. How do we deal with multiple formats?
    * 2. Read the stocks DF and parse the dates
    * */

  // 1 - parse the DF multiple times, then union the small DFs - Johns thoughts - parse the dates in different columns and use the ones that work.


  // 2
  val stocksDF = spark.read
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.withColumn("myDate", to_date($"date", "MMM d yyyy")).show()

  // Structures

  // 1 - with col operators
  moviesDF
    .select($"Title", struct($"US_Gross", $"Worldwide_Gross").as("Profit"))
    .withColumn("Yehaw", $"Profit".getField("US_Gross")).as("UsGrossy")

  // 1 - with col operators
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit" ).show()

  // Arrays
  val moviesWithWords = moviesDF.select($"Title", split($"Title", " |,").as("Title_Words"))
  moviesWithWords.select(
    $"Title",
    $"Title_Words",
    expr("Title_words[0]"),
    size($"Title_Words"),
    array_contains($"Title_Words", "Love")
  ).show(false)
}
