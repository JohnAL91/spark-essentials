package part2dataframes

import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.{Column, SparkSession}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("ColumnsAndExpressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns (projecting)
  val firstColumn: Column = carsDF.col("Name")

  // Selecting
  var carNamesDF = carsDF.select(firstColumn)

  // various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("name"),
    col("Acceleration"),
    column("Weight_in_lbs"), // col / columns do the exact same thing
    'Year, // Scala Symbol, auto-converted to column
    $"HorsePower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // Select with plain column names (can't use these same time as above)
  carsDF.select("Name", "Year")

  // Select is a NARROW TRANSFORMATION since every node that has a partition of the DF
  // has exactly one corresponding output partition in the resulting dataframe

  // EXPRESSIONS
  val simplesExpression = carsDF.col("Weight_in_lbs") // returns a column object which is a subtype of an expression
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr (every line is an expr!)
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // Adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // Renaming a col
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // Careful with column names, use back tics if you use spaces
  carsWithColumnRenamed.selectExpr("`Weight in pounds`").show()
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'") // Single quotes like SQL
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") =!= "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") =!= "USA").and(col("Horsepower") > 150))
  val americanPowerfulCarsDF22 = carsDF.filter(col("Origin") =!= "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150") // Single quotes like SQL

  //union = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // Works only if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  /**
    * Exercises
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the three gross (US_Gross + Worldwide_Gross + DVD sales) "US_Gross":37402877,"Worldwide_Gross":37402877,"US_DVD_Sales":null
    * 3. Select all COMEDY movies (Major_Genre) with IMDB rating above 6
    *
    * Use as many versions as possible for all exercises
    * */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // 1
  moviesDF.select("Title", "US_Gross").show()
  moviesDF.selectExpr("Title", "US_Gross").show()
  moviesDF.select(expr("Title"), expr("US_Gross")).show()
  moviesDF.select(col("Title"), col("US_Gross")).show()
  moviesDF.select(column("Title"), column("US_Gross")).show()
  moviesDF.select(moviesDF.col("Title"), moviesDF.col("US_Gross")).show()
  moviesDF.select($"Title", $"US_Gross").show()
  moviesDF.select('Title, 'US_Gross).show()

  // 2
  moviesDF.withColumn("Total_Gross", 'US_Gross + 'Worldwide_Gross + 'US_DVD_Sales).show()
  moviesDF.withColumn("Total_Gross", expr("US_Gross + Worldwide_Gross + US_DVD_Sales")).show()
  moviesDF.select($"*", expr("US_Gross + Worldwide_Gross + US_DVD_Sales").as("Total_Gross")).show()
  moviesDF.select($"*", expr("US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross")).show()

  // 3
  moviesDF.filter($"Major_Genre" === "Comedy").filter($"IMDB_Rating" > 6.0).select($"Title")
  moviesDF.filter($"Major_Genre" === "Comedy" && $"IMDB_Rating" > 6.0).select($"Title")
  moviesDF.filter($"Major_Genre" === "Comedy" and $"IMDB_Rating" > 6.0).select($"Title")
  moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6.0").select($"Title").show()

}
