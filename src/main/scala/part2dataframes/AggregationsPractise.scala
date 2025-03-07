package part2dataframes

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object AggregationsPractise extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.json")

  val genresDf = moviesDf.select((count(col("Major_Genre"))).as("Count_Genre"))
  val genresDf1 = moviesDf.select((countDistinct(col("Major_Genre"))).as("Count_Genre_distinct"))
  val genresDf2 = moviesDf.select(count("*").as("Total_rows"))

  // same as count we have min, max, sum, avg, mean, stddev functions we can use this inside select

  // Grouping
  val groupedGenre = moviesDf
    .groupBy("Major_Genre")
    .count()

  val groupedGenre1 = moviesDf
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating").alias("Avg") // This won't  work need to use agg and mention inside tht

  val groupedGenre2 = moviesDf
    .groupBy("Major_Genre")
    .agg(
      avg("IMDB_Rating").as("Average_rating"),
      count("Major_Genre").as("No_of_Genres")
    )
    .orderBy("Average_rating")


  //  genresDf.show()
  //  genresDf1.show()
  //  genresDf2.show()
  //  groupedGenre.show()
  //  groupedGenre1.show()
  //  groupedGenre2.show()

//  val profitsSumDf = moviesDf.select(col("Title"), (col("Production_Budget") - col("Worldwide_Gross") + col("US_Gross")).as("Profit"))
  val profitsSumDf = moviesDf.select( sum(col("Worldwide_Gross") + col("US_Gross")).as("Total_sale"))
  profitsSumDf.show()

  val directorsDf = moviesDf.select(countDistinct(col("Director")).as("No_of_distinct_directors"))
  directorsDf.show()

  val meanDf = moviesDf.select(mean("US_Gross").as("Mean"), stddev("US_Gross").as("SD"))
  meanDf.show()

  val perDirector = moviesDf.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("US_Gross").as("Avg_Gross_Revenue")
    ).orderBy(col("Avg_IMDB_Rating").desc)

  perDirector.show()
}
