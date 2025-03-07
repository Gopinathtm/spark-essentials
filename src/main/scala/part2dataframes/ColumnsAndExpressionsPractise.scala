package part2dataframes

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object ColumnsAndExpressionsPractise extends App {

  val spark = SparkSession.builder()
    .appName("Columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val cardDf = spark.read
    .format("json")
    .option("dateFormat", "yyyy-MM-dd") // refer this for date patterns https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    .option("inferSchema", true)
    .load("src/main/resources/data/cars.json")

  val carsDfCol: Column = cardDf.col("Name")
  val carsDf1: Dataset[Row] = cardDf.select(carsDfCol).as("New_Name")
  val carsDf2: DataFrame = cardDf.select((col("Horsepower") * 2).as("Double_the_HP"))
  val carsDf3: DataFrame = cardDf.selectExpr("Horsepower * 2 AS Horse_power_doubled")
  val carsDf4: Dataset[Row] = cardDf.filter(col("Horsepower") > 150)
  val carsDf5 = spark.read
    .format("json")
    .option("inferSchema", true)
    .load("src/main/resources/data/more_cars.json")
  val carsDf6 = cardDf.union(carsDf5)
  //  carsDf1.show()
  //  carsDf2.show()
  //  carsDf3.show()
  //  carsDf4.show()
  //  println(carsDf6.count())
  //  println(cardDf.count())

  val colums = Seq(col("a"), col("b"))

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.json")

  val selectedColumns1 = moviesDf.select("Title", "Worldwide_Gross")
  val selectedColumns2 = moviesDf.select(col("Title"), col("Worldwide_Gross"))
  val totalSales = (col("Worldwide_Gross") + col("US_Gross")).as("Total_sale")
  val totalProfitDf = moviesDf.select(col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    totalSales,
  )

  val moviesDf2 = moviesDf.selectExpr("Title", "US_Gross", "Worldwide_Gross",
    "US_DVD_Sales", "US_Gross + Worldwide_Gross AS Total") //.show()
  moviesDf2.show()
  selectedColumns1.show()
  selectedColumns2.show()
  totalProfitDf.show()

  val selectComedyMoviesExpr = (col("Major_Genre") === "Comedy").and(col("IMDB_Rating") > 7)

  val comedyMoviesdDf = moviesDf.filter(selectComedyMoviesExpr)
  val comedyMoviesdDfs = moviesDf.filter(selectComedyMoviesExpr)

  comedyMoviesdDf.show()


}
