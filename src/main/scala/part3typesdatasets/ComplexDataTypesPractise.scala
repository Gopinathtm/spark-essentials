package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object ComplexDataTypesPractise extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    //    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .json("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.json")

  // Dates

  val yearConversion = when(year(col("Actual_Date")) > 2030, date_sub(col("Actual_Date"), 36525)).otherwise(col("Actual_Date"))

//  val a = col()
  val moviesWithReleaseDates = moviesDf.select(col("Title"), to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Date"))
    .select(col("Title"), yearConversion.as("R_date"))
    .withColumn("Today", current_date())
    .withColumn("Now", current_timestamp())
    .withColumn("DifferenceInYears", year(current_date()) - year(col("R_date")))

  moviesWithReleaseDates.select("*").where(col("R_date").isNull) // if it didn't parse the date(i.e different format) then it will show as null

  // csv flags
  val csvSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val stocksDf = spark.read
    .schema(csvSchema)
    .option("dateFormat", "MMM d yyyy") //https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/stocks.csv")

  stocksDf.select("*").where(col("date").isNull)

  moviesDf.select(split(col("Title")," ").as("Array_of_cols")).show()

}
