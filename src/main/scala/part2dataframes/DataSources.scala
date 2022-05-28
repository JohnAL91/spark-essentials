package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("DataSources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType), // If the incorrect datformat is given while reading, it will return a null
    StructField("Origin", StringType)
  ))

 /*
  Reading a DF:
  - format
  - schema (optional)
  - path
  - zero or more options
  */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // failFast - will fail if we get malformed record | dropMalformed ignores - faulty rows | permissive - the default, allows faulty rows
    .option("path", "src/main/resources/data/cars.json") // alternative way to specify the path
    .load()

  // With options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .schema(carsSchema)
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /* Writing DFs
     - format
     - save mode = overwrite, append, ignore, errorIfExists
     - path
     - zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  // JSON FLAGS
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd") // When including date format we must supply a schema, if we don't spark will try and use its standard ISO format. If spark fails parsing, it will put null.
    .option("allowSingleQuotes", "true") // needed if json is formated with single quotes
    .option("compression", "uncompressed") // The default value is uncompressed, else bzip2, gzip, lz<4, snappym deflate
    .json("src/main/resources/data/cars_dupe.json")

  // CSV FLAGS
  val stockSchema = StructType(Seq(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType),
  ))

  spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true") // the names of the columns must be exactly the same as the headers in the file
    .option("sep", ",") // seperator
    .option("nullValue", "") // No nulls in CSV - will parse "" as null
    .csv("src/main/resources/data/stocks.csv")

  // PARQUET FLAGS - the default storage format for dataframes
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet") // Can use save as it parquet is the default

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  private val url = "jdbc:postgresql://localhost:5432/rtjvm"
  private val driver = "org.postgresql.Driver"
  private val user = "docker"
  private val password = "docker"
  // Reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

    employeesDF.show()

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file (csv)
    * - snappy parquet
    * - table "public.movies" in the DB
    */

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .option("sep","\t")
    .option("header", "true")
    .mode(SaveMode.Overwrite)
    .csv("src/main/resources/data/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()

  val test = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .load()
    .show()
}
