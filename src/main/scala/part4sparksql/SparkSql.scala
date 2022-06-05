package part4sparksql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import part2dataframes.myDbOptions

object SparkSql extends App {
  /*
    - Spark SQL has the concept of "database", "table", "view"
    - Allows accessing DataFrames as tables
    - Identical in terms of storage and partitioning
   */

  val spark = SparkSession.builder()
    .appName("Spark QL practice")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse") // SETS THE SOURCE OF NEW "DATABASES" USING SPARK SQL. Default is /spark-warehouse.
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select($"Name").filter($"Origin" === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      | Select Name FROM cars where Origin = 'USA'
    """.stripMargin
  )

  // We can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
  databasesDF.show()

  // Transfer tables from a DB to Spark tables
  def readTable(tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .options(myDbOptions)
      .option("dbtable", s"public.$tableName")
      .load()
  }

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach {tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if(shouldWriteToWarehouse)
      tableDF.write.option("path", s"src/main/resources/warehouse/rtjvm.db/$tableName").mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

//  transferTables(List(
//    "employees",
//    "departments",
//    "titles",
//    "dept_emp",
//    "salaries",
//    "dept_manager"
//  ))

  // read a DF from loaded tables - converts a table loaded in memory to a df. // THIS DOES NOT WORK
//  val employeesDF2 = spark.read.table("employees")
//  employeesDF2.createOrReplaceTempView("employees")
//  employeesDF2.show()

  /**
    * Exercises
    * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2001 (see hired_date)
    * 3. Show the average salaries for the employees hired in between those date grouped by department.
    * 4. Show the name of the best-paying department for employees hired in between those dates.
    * */

    // 1
//  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
//  moviesDF.write.mode(SaveMode.Overwrite).option("path", s"src/main/resources/warehouse/rtjvm.db/movies").saveAsTable("movies")

  // 2 Count how many employees were hired in between Jan 1 1999 and Jan 1 2001 (see hired_date)
  spark.sql(
    """
      |select count(*)
      |from employees e
      |where hire_date > '1999-01-01' AND hire_date < '2000-01-01'
      |""".stripMargin).show()

  /*
  // 3 Show the average salaries for the employees hired in between those date grouped by department.
  spark.sql(
    """
      |select dept_no, avg(salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' AND e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |group by de.dept_no
      |""".stripMargin).show()

  // My way
  spark.sql(
    """
      |SELECT dept_no, avg(salary)
      |FROM employees e
      |LEFT JOIN dept_emp d ON e.emp_no = d.emp_no
      |LEFT JOIN salaries s ON e.emp_no = s.emp_no
      |WHERE hire_date >= '1999-01-01' AND hire_date <= '2001-01-01'
      |GROUP BY d.dept_no
      |""".stripMargin).show()


  // 4 Show the name of the best-paying department for employees hired in between those dates.
  spark.sql(
    """
      |select avg(s.salary) as payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' AND e.hire_date < '2000-01-01'
      |and e.emp_no = de.emp_no
      |and e.emp_no = s.emp_no
      |and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
      |""".stripMargin).show()


  // Han snackar om att göra joins på detta sätt.. Detta är en crossjoin med filter. Se mitt test nedan.
  spark.sql(
    """
      |select *
      |from employees e, dept_emp d
      |where e.emp_no = d.emp_no
      |""".stripMargin)

  val t1 = List((1,2,3), (4,5,6)).toDF("c1", "c2", "c3")
  val t2 = List(("a","b","c"), ("e","f","g")).toDF("co1", "co2", "co3")

  t1.createOrReplaceTempView("t1")
  t2.createOrReplaceTempView("t2")

  spark.sql(
    """
      |Select *
      |FROM t1, t2
      |""".stripMargin
  )
*/
}
