package part3typesdatasets

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object ManagingNull extends App {

  /* When giving a schema (StructType) and setting Nullable=false/true, that is actually not a constraint -
  but rather a marker for Spark to optimize for nulls. Can lead to exceptions or data errors if broken
   */

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // select the first non-null value
  moviesDF.select(
    $"Title",
    $"Rotten_Tomatoes_Rating",
    $"IMDB_Rating",
    coalesce($"Rotten_Tomatoes_Rating", $"IMDB_Rating"*10)
  ).show()

  // checking for nulls
  moviesDF.filter($"Rotten_Tomatoes_Rating".isNull)

  // nulls when ordering
  moviesDF.orderBy($"IMDB_Rating".desc_nulls_last)

  // removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // Returns a new DataFrame that drops rows containing any null or NaN values.

  // replace nulls
  moviesDF.na.fill(0, List("Rotten_Tomatoes_Rating", "IMDB_Rating"))
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  )).show()

  // Complex operations
  moviesDF.selectExpr(
    "Title",
    "Rotten_Tomatoes_Rating",
    "IMDB_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // ifnull() similar to coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same thing
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if(first != null) second else third
  ).show()

}
