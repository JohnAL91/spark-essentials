package part5lowlevel

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {

  /*
    RDD - Resilient Distributed Datasets
    - They are distributed typed collections of JVM objects (Very similarly to datasets)
    - The underlying first citizens of spark: all higher-level APIs reduce to RDDs.
    - Can be highly optimized
      - we can control partitioning
      - order of elements can be controlled
      - order of operations matters for performance

    - Hard to work with
      - for complex operations, need to know the internals of spark
      - poor APIs for quick data processing

    - For 99% of operations, use the Dataframe/Dataset APIs - Spark is usually smart enough to do the
    optimization for you.

    RDDs vs Datasets (remember, Dataframes == Dataset[Row]
    In common
    - collection API: map, flatMap, filter, take, reduce
    - union, count, distinct
    - groupBy, sortBY

    RDDs over Datasets
    - partition control: repartition, coalesce, partitioner, zipPartitions, mapPartitions
    - operation control: checkpoint, isCheckpointed, localCheckpoint, cache
    - storage control: cache, getStorageLevel, persist

    Datasets over RDDs
    - select and join
    - Spark planning(optimization before running the code

    For 99% of operations, use the Dataframe/Dataset APIs
   */

  val spark = SparkSession.builder()
    .appName("Intro to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext // entrypoint for RDDs

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from file
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String): Seq[StockValue] = {
    val src = Source.fromFile(filename) // Source from scala.io!
    val result = src.getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
    src.close()

    result
  }

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // A bit weird, but gets rid of the header row
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  import spark.implicits._
//  val stocksDF = spark.read
//    .option("header", "true")
//    .csv("src/main/resources/data/stocks.csv")
//  stocksDF.show()
//  val stocksDS = stocksDF.as[StockValue] // Crashes - org.apache.spark.sql.AnalysisException: Cannot up cast `price` from string to double.
//  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // Lose the type information as DF does not have type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // Get to keep the type info

  // Transformations
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transform
  val msCount = msftRDD.count() // eager ACTION

  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue]((stockA: StockValue, stockB: StockValue) => stockA.price < stockB.price) // Three ways of telling the type to the compiler (only requires one of them)

  val minMsft = msftRDD.min() // Action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol) // Very expensive - involves shuffles

  // Partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  /*
   Repartitioning is expensive! Involves shuffles. Best practice: Partition EARLY, then process after that.
   Size of partition should be 10-100MB.
   */

  // Coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // Will NOT involve a true shuffling (15 parts will stay the same, the other 15 will move to them!)
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")


  /**
    * Exercises
    * 1. Read the movies json as an RDD of the case class Movie
    * 2. Show the distinct genres as an RDD
    * 3. Select all the movies in the drama genre with IMDB rating > 6.0
    * 4. Show the average rating of movies by genre
    */

  case class Movie(title: String, genre: String, rating: Double)

  // 1. Read the movies json as an RDD of the case class Movie
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select($"Title", $"Major_Genre" as "genre", $"IMDB_rating" as "rating")
    .filter($"genre".isNotNull && $"rating".isNotNull)
    .as[Movie].rdd

  // 2. Show the distinct genres as an RDD
  val genresRDD = moviesRDD.map(_.genre).distinct()

  //  3. Select all the movies in the drama genre with IMDB rating > 6.0
  val dramaRDD = moviesRDD.filter(mov => mov.genre.toLowerCase == "drama" && mov.rating > 6.0)

  //  4. Show the average rating of movies by genre
  case class GenreAvgRating(genre: String, rating: Double)
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case(genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  genresRDD.toDF.show()
  dramaRDD.toDF.show()
  avgRatingByGenreRDD.toDF.show()
  moviesRDD.toDF.groupBy($"genre").avg("rating").show() // For comparison
}
