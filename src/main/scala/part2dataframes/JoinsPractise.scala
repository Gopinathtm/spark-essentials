package part2dataframes

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object JoinsPractise extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val bandsDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")
  val guitarDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val guitarPlayer = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")


  val joinCondition =  bandsDf.col("id") === guitarPlayer.col("band")
//  val innerJoinDf = bandsDf.join(guitarPlayer,joinCondition, "inner")
  val a = guitarPlayer.withColumnRenamed("band", "band_id")
  val innerJoinDf = bandsDf.join(guitarPlayer, "id", "inner")
  val leftOuterJoinDf = bandsDf.join(guitarPlayer, joinCondition, "left_outer")
  val rightOuterJoinDf = bandsDf.join(guitarPlayer, joinCondition, "right_outer")
  val outerJoinDf = bandsDf.join(guitarPlayer,joinCondition, "outer")
  innerJoinDf.show()
//  leftOuterJoinDf.show()
//  rightOuterJoinDf.show()
//  outerJoinDf.show()

  guitarPlayer.join(guitarDf.withColumnRenamed("id", "guitar_id"), expr("array_contains(guitars,guitar_id)")).show()


  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val employeeTable = "public.employees"
  val salarytable = "public.salaries"
  val managertable = "public.dept_manager"
  val titletable = "public.titles"
  val user = "gopinathtm"
  val password = "buddi@tmp1"
  val driver = "org.postgresql.Driver"
  val employeeDb = spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", employeeTable)
    .option("user", user)
    .option("password", password)
    .option("driver", driver)
    .load()
  val salaryDb = spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", salarytable)
    .option("user", user)
    .option("password", password)
    .option("driver", driver)
    .load()
  val managerDb = spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", managertable)
    .option("user", user)
    .option("password", password)
    .option("driver", driver)
    .load()

  val titlesDb = spark.read.format("jdbc")
    .option("url", url)
    .option("dbtable", titletable)
    .option("user", user)
    .option("password", password)
    .option("driver", driver)
    .load()

  val maxSalaryPerEmployeeDf = salaryDb.groupBy("emp_no").agg(max(col("salary")).as("maxSalary"))
  val employeeMaxSalaryDf = employeeDb.join(maxSalaryPerEmployeeDf, "emp_no")
  employeeMaxSalaryDf.show()

  val employeeNeverManageDf = employeeDb.join(managerDb, "emp_no", "left_anti")
  employeeNeverManageDf.show()

  val highSalaryTitle = employeeDb.join(salaryDb.join(titlesDb.withColumnsRenamed(Map("from_date"-> "titles_from_date", "to_date"-> "titles_to_date")), "emp_no"), "emp_no")
    .groupBy(col("emp_no"))
    .agg(max(col("salary")).as("Max_salary"),
      max(col("titles_to_date")).as("working_date")
    ).orderBy(col("Max_salary").desc).offset(10)
  highSalaryTitle.show()

//  val mostRecentJobTitles = titlesDb.groupBy("emp_no", "title").agg(max(col("to_date")).as("recent_job_title"))
//  mostRecentJobTitles.show()
//val bestPaidEmployeesDf = employeeMaxSalaryDf.orderBy(col("Max_salary").desc).limit(10)
//  bestPaidEmployeesDf.show()
//  val bestPaidDf = bestPaidEmployeesDf.join(mostRecentJobTitles, "emp_no")
//  bestPaidDf.show()

  val mostRecentJobTitlesDF = titlesDb.groupBy("emp_no", "title").agg(max("to_date"))
//  val bestPaidEmployeesDF = employeeMaxSalaryDf.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = employeeMaxSalaryDf.join(mostRecentJobTitlesDF, "emp_no").orderBy(col("maxSalary").desc).limit(10)

  bestPaidJobsDF.show()

}
