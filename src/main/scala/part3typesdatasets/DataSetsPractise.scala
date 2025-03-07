package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.sql.Date

object DataSetsPractise extends App {

  val spark = SparkSession.builder()
    .appName("Columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.json")

  val numbersDf = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/numbers.csv")

  val numbersDataSet = numbersDf.as[Int]
  numbersDataSet.show()


  case class Car(Name: String,
                 Miles_per_Gallon: Option[Double],
                 Cylinders: Long,
                 Displacement: Double,
                 Horsepower: Option[Long],
                 Weight_in_lbs: Long,
                 Acceleration: Double,
                 Year: Date,
                 Origin: String)

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  val cardDf = spark.read
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd")
    .json("src/main/resources/data/cars.json")

  val carsDs = cardDf.as[Car]

  val noOfCars = carsDs.count()
  val noOfPowerFullCars = carsDs.filter(car => car.Horsepower.getOrElse(0L) > 140  ).count()
  val averageHp = carsDs.map(car => car.Horsepower.getOrElse(0L)).reduce(_ + _) / noOfCars

  println(s"noOfCars $noOfCars" )
  println(s"noOfPowerFullCars $noOfPowerFullCars")
  println(s"averageHp $averageHp")

}
