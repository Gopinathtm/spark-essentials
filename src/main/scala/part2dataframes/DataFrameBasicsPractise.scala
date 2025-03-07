package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFrameBasicsPractise extends App {

  // Creating a spark session
  val spark = SparkSession.builder()
    .appName("DataFrameBasics")
    .config("spark.master", "local")
    .getOrCreate()

  val firstDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  firstDf.show()
  firstDf.printSchema()
  //  root
  //  |-- Acceleration: double (nullable = true)
  //  |-- Cylinders: long (nullable = true)
  //  |-- Displacement: double (nullable = true)
  //  |-- Horsepower: long (nullable = true)
  //  |-- Miles_per_Gallon: double (nullable = true)
  //  |-- Name: string (nullable = true)
  //  |-- Origin: string (nullable = true)
  //  |-- Weight_in_lbs: long (nullable = true)
  //  |-- Year: string (nullable = true)

  firstDf.take(10).foreach(println)
  //  it prints the array of rows, row may contain any value
  //  [12.0,8,307.0,130,18.0,chevrolet chevelle malibu,USA,3504,1970-01-01]
  //  [11.5,8,350.0,165,15.0,buick skylark 320,USA,3693,1970-01-01]

  //  spark data types : LongType, StringType and so on
  //  Can be inferred from dataframes only can't create a longType and assign it to a val
  val longType = LongType

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carDFSchema = firstDf.schema
  println(carDFSchema)

  val carDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema) // it should be of structType
    .load("src/main/resources/data/cars.json")

  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )
  val g = Seq((1, "gopi"), (2, "shree"))

  val manualDF = spark.createDataFrame(cars) // schema auto inferred, no column names it will be _1, _2 and so on
  // df have schema but rows do not have

  import spark.implicits._
  // here we can mention column names
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  manualDF.printSchema()
  manualCarsDFWithImplicits.printSchema()


  /*
  Exercise
   */

  val mobiles = Seq(
    ("samsung", "m12", 5.6, 24),
    ("xiaomi", "note 16", 6.2, 32),
    ("apple", "x10", 6.2, 24),
    ("nokia", "1100", 1.2, 10),
  )

  val mobRdd = spark.sparkContext.parallelize(mobiles)

  val mobilesDF = mobRdd.toDF("Make", "Model", "Screen Size", "Camera")
  mobilesDF.show()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", value = true)
    .load("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(s"\n\tThis movies df has ${moviesDF.count()} rows")



}
