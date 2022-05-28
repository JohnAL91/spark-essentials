package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  /*
    Groupings are WIDE TRANSFORMATIONS - one/more input partitions => one/more output partitions
    These operations are called shuffles (data is being moved between the different nodes of the cluster)
    Has big performance impact
  */

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // Counting
  import spark.implicits._
  val genresCountDF = moviesDF.select(count($"Major_Genre")) // Counts all the values except null
  moviesDF.selectExpr("count(Major_Genre)").show()

  // counting all
  moviesDF.select(count($"*")).show() // counts all the rows and will include nulls

  // count distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count - gives an approximate, very good for very large data
  moviesDF.select(approx_count_distinct($"Major_Genre")).show()

  // min and max
  moviesDF.select(min($"IMDB_Rating"))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum("US_Gross")).show()
  moviesDF.selectExpr("sum(US_Gross)").show()

  // avg
  moviesDF.select(avg("Rotten_Tomatoes_Rating")).show()
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

  // data science
  moviesDF.select(
    mean("Rotten_Tomatoes_Rating"),
    stddev("Rotten_Tomatoes_Rating")
  ).show()

  // Grouping
  moviesDF
    .groupBy($"Major_Genre") // Also iucludes null
    .count() // select count(*) from moviesDF group by Major_Genre

  moviesDF
    .groupBy($"Major_Genre")
    .avg("IMDB_Rating")

  moviesDF
    .groupBy($"Major_Genre")
    .agg(
      count("*") as "N_Movies",
      avg("IMDB_Rating") as "Avg_Rating"
    )
    .orderBy("Avg_Rating").show()

  /** EXERCISES
    *
    * 1. sum up all the profits of all the movies in the DF
    * 2. count how many distinct directors we have
    * 3. show the mean and std of US gross revenue
    * 4. compute the average imdb rating and the average us gross revenue per director
    *
    * {"Title":"In the Company of Men","US_Gross":2883661,"Worldwide_Gross":2883661,"US_DVD_Sales":null,"Production_Budget":25000,"Release_Date":"1-Aug-97","MPAA_Rating":"R","Running_Time_min":null,"Distributor":"Sony Pictures Classics","Source":"Based on Play","Major_Genre":"Drama","Creative_Type":"Contemporary Fiction","Director":"Neil LaBute","Rotten_Tomatoes_Rating":89,"IMDB_Rating":7.2,"IMDB_Votes":7601}
    * */


  moviesDF.select(sum($"US_Gross" + $"Worldwide_Gross" + $"US_DVD_Sales")).show()
  moviesDF.select(countDistinct("Director"))
//  println(moviesDF.select("Director").distinct().count())
  moviesDF.select(mean("US_Gross"), stddev("US_Gross")).show()
  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("avg_rating"),
      avg("US_Gross").as("avg_gross")
    ).orderBy($"avg_rating".desc).show()

}
