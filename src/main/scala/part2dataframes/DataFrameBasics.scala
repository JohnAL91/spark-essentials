package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFrameBasics extends App {

  /*
  DataFrames - distributed collection of Rows confirming to a schema (list describing column names and types)
  Can be looked at as at table where each node in the cluster gets some of the rows for computation

  The types used are known to Spark, NOT AT COMPILE TIME (at runtime). We will learn how we can use datasets for type saftey.
  All rows have the same structure. Arbitrary number of columns.

  Spark uses partitioning - splits the data into files distributed between nodes in the cluster.
  It impacts the processing parallelism.

  DFs are immutable

  Transformations
  - narrow = one input partition contributes to at most one output partition (e.g. map)
  - wide = input partitions (one or more) create many output partitions (e.g. sort)

  Shuffle = data exchange between cluster nodes (massive performance topic)
  - occurs in wide transformations

  Computing DataFrames
  Lazy evaluation
  - spark waits until the last moment to execute the DF transformations
  Planning
  - Spark compiles the DF transformations into a graph before running any code
  - The logical plan = DF dependency graph + narrow/wide transformations sequence
  - The physical plan = optimized sequence of steps for nodes in the cluster
  - It is able to optimize based on these plans

  Transformations vs Actions
  - transformations describe how new DFs are obtained
  - actions actually start executing spark code (like show() or count())
 */

  // Creating a spark session
  val spark = SparkSession.builder()
    .appName("DataFrameBasics")
    .config("spark.master", "local")
    .getOrCreate()

  // Reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  firstDF.show()
  firstDF.printSchema()

  // Get rows
  val rows: Array[Row] = firstDF.take(10)
  rows.foreach(println) // Every row is an array of things/data

  // spark types
  val longType = LongType // Notice - these are case objects (singletons)
  val doubleType = DoubleType

  // Schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // obtain schema of a DF
  val carsDfSchema = firstDF.schema
  println(carsDfSchema)

  // We shouldn't really infer schema, but rather define the schema and force the DF to conform to that. Else it might be inferred wrongly.

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDfSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // Schema auto-inferred

  // note: DFs have schemas, rows do not

  // create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOfOrigin")

  manualCarsDF.printSchema() // Gets column names like; _1, _2, _3...
  manualCarsDFWithImplicits.printSchema()

  /**
    * Exercise
    * 1) Create a manual DF describing smart phones
    *  - make
    *  - model
    *  - screen dimensions
    *  - camera megapixels
    *
    *  2) Read another file from the data folder, for example movies.json
    *  - print its schema
    *  - count the number of rows, call count()
    */


  val smartPhonesDf = Seq(
    ("Samsung", 3.0, "300x500", "100000"),
    ("Telefonia", 1.2, "300x500", "600000"),
    ("CoolPhone", 3.33, "300x700", "1000000"),
  ).toDF("make", "model", "screenDim", "mPixles")

  smartPhonesDf.show()
  smartPhonesDf.printSchema()

  val moviesDF = spark.read
    .format("json")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(moviesDF.count())

}
