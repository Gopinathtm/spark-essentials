package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object ManagingNullsPractise extends App {

  val spark = SparkSession.builder()
    .appName("Managing null")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy") // doesn't infer's as date
    .json("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.json")



  moviesDf.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10, lit(10)).as("Rating"))

  moviesDf.where(col("Rotten_Tomatoes_Rating").isNull)


  moviesDf.orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_last)

  // dropping nulls
  moviesDf.na.drop(minNonNulls = 1, cols = Seq("Rotten_Tomatoes_Rating", "IMDB_Rating"))

    // If we do this we won't get any rows because the above method removes if both values are null
//    .where(col("Rotten_Tomatoes_Rating").isNull and col("IMDB_Rating").isNull).show()

}
