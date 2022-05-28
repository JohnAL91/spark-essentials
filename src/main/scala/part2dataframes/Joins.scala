package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {

  /*
   Joins are WIDE TRANSFORMATIONS
  */

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val guitaristsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with NULLS where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with NULLS where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer = everything in the inner join + all the rows in the BOTH table, with NULLS where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins - will show only the data in the left dataframe for which there is a match on the right table
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins - will show only the data in the left dataframe for which there IS NOT a match on the right table
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // things to bear in mind
  // guitaristBandsDF.select("id","band").show()  // Will crash, id is ambigous

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristBandsDF.drop(bandsDF.col("id")) // Spark maintains unique identifiers for all the columns it operates on.

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandID")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandID"))

  // using complex types (guitarPlayers has field guitars as array)
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /**
    * Exercise
    * - show all employees and their max salaries
    * - show all employees who were never managers
    * - find the job titles of the best paid 10 employees in the company
    */

  val employeesDF = spark.read
    .format("jdbc")
    .options(myDbOptions)
    .option("dbtable", "public.employees")
    .load()
  val salariesDF = spark.read
    .format("jdbc")
    .options(myDbOptions)
    .option("dbtable", "public.salaries")
    .load()
  val titlesDF = spark.read
    .format("jdbc")
    .options(myDbOptions)
    .option("dbtable", "public.titles")
    .load()
  val depManagerDF = spark.read
    .format("jdbc")
    .options(myDbOptions)
    .option("dbtable", "public.dept_manager")
    .load()

  import spark.implicits._
  val maxSalaries = salariesDF.groupBy($"emp_no").agg(max("salary") as "max_salary")
  employeesDF.join(maxSalaries, Seq("emp_no"), "left").show()

  employeesDF.join(depManagerDF, Seq("emp_no"), "left_anti").show()

  val latesFromDate = salariesDF.groupBy("emp_no").agg(max("from_date").as("fromDateMax"))
  val latestSalaries = salariesDF.join(latesFromDate, latesFromDate("fromDateMax") === salariesDF("from_date") && latesFromDate("emp_no") === salariesDF("emp_no"), "left_semi")
  latestSalaries.join(titlesDF, Seq("emp_no")).orderBy($"Salary".desc).show(10)
}
