package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object CommonTypesPractise extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.json")

  // Adding a plain value column to the df (Any)

  moviesDf.select(col("Title"), lit("chuma").as("Extra_column")).show()

  //Boolean
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val highRating = col("IMDB_Rating") > 7

  moviesDf.where(dramaFilter and highRating)

  moviesDf.withColumn("dramaFilter", dramaFilter).where("dramaFilter")

  moviesDf.withColumn("dramaFilter", dramaFilter).where(not(col("dramaFilter")))

  val cardDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Strings
  cardDf.select(initcap(col("Name")).as("Capitalized_Name")).show()

  val regexString = "vw|volkswagen"

  val vwDf =
    cardDf.select(col("Name"), regexp_extract(col("Name"), regexString, 0).as("Matched_String"))
      .where(col("Matched_String") notEqual "").drop("Matched_String")

  vwDf.select(col("Name"), regexp_replace(col("Name"), regexString, "gopi").as("replaced_col"))


  val carsList = List("volkswagen", "vw rabbit", "Benz", "Ford")

  val carsRegexString = carsList.mkString("|").toLowerCase

//  println(carsRegexString)

  cardDf.filter(col("Name").isInCollection(carsList).as("best_cars")).show()
  cardDf.select(col("Name"), regexp_extract(col("Name"), regexString, 0).as("matched_cars")) //.show()
    .where(col("matched_cars") notEqual "").drop("matched_cars").show()
//  println(carsRegexString)

  val containsListFilter = carsList.map(c => col("Name").contains(c.toLowerCase))
  val combinedFilter = containsListFilter.fold(lit(false))((oldFilter, newFilter) => oldFilter or newFilter)
  cardDf.where(combinedFilter).show()


}
