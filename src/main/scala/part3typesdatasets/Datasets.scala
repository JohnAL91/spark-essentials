package part3typesdatasets

import org.apache.spark.sql.functions.{array_contains, avg, expr}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, KeyValueGroupedDataset, SparkSession}

import java.sql.Date

object Datasets extends App {

  /*
    Datasets most usefule when
    - We want to maintain type information
    - we want clean concise code
    - our filters/transformations are hard to express in DF or SQL

    Avoid when
    - Performance is critical: Spark can't optimize transformations - all the transforms are plain scala objects that
    will be evaluated at runtime, that is, after spark has the chance to plan for the ops in advance. Will have to eval
    the filters/transforms on row-by-row basis which is very slow

    All DataFrames are actually Dataset of Rows!
    type DataFrame = Dataset[Row]
   */

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

//  numbersDF.filter(_ > 100) // would like to do this, but can't with a df

  // Convert a DF to a Dataset
  // The encoder specified below turns a row in a df into an Int (ctrl+q on .as says using primitives the first col will be used
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt // THIS IS AUTOMATICALLY GOTTEN WITH import spark.implicits._
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.filter(_ < 100)

  // dataset of a complex type
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double], // To enable null values, use option. Else constraint not null.
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")
//  implicit val carEncoder = Encoders.product[Car] // Case classes extends product
  import spark.implicits._ // this imports all the encoders needed
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100).show()
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()

  /**
    *
    * Exercise
    *
    * 1. Count how many cars we have
    * 2. Count how many powerful cars we have (Horsepower > 140)
    * 3. Average Horsepower for the entire dataset
    */

  val carsCount: Long = carsDS.count
  println(carsCount)
  val x = carsDS.map(_.Horsepower.getOrElse(0L))
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count()) // just 0 is an int, but horsepower is a long.
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)
  carsDS.select(avg($"Horsepower")).show()

  // SECOND VIDEO STARTS HERE

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitarsWithRenamedType.json").as[Guitar]
  val guitarPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayerDS.joinWith(bandDS, guitarPlayerDS("band") === bandDS("id"), "inner") // Returns a Dataset[Tuple of the joined DS case classes]
  guitarPlayerBandsDS.show() // you can rename the columns from _1 and _2

  /**
    * Exercise
    *
    * 1. join the guitarsDS and guitarPlayersDS (hint use array_contains) using outer join
    *
    * */

  val myJoin: Dataset[(Guitar, GuitarPlayer)] = guitarsDS.joinWith(guitarPlayerDS, array_contains(guitarPlayerDS("guitars"), guitarsDS("id")), "outer")

  // Groupings
  val carsGroupedByOrigin: KeyValueGroupedDataset[String, Car] = carsDS.groupByKey(_.Origin)
  val myCount: Dataset[(String, Long)] = carsGroupedByOrigin.count()
  myCount.show()

  // joins and groups are WIDE transformations - will involve shuffle operations
}
