package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSourcesPractise extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()


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

  /*
  Reading a dataframe
  - format
  - schema or inferSchema = true
  - zero or more options
  - load path of the file ( can be used as option)
   */
  //  val carDFWithSchema = spark.read
  //    .format("json")
  //    .schema(carsSchema) // it should be of structType
  //    //    .option("mode", "failFast") // if any rows doesn't match the schema throws error, but in default mode permissive it ignores the row
  //    .load("src/main/resources/data/cars.json")
  //
  //  carDFWithSchema.show()

  /*
Writing a dataframe
- format
- save mode (overWrite, ignore, errorIfExists)
- zero or more options
- save path of the file ( can be used as option)
 */

  //  carDFWithSchema.write
  //    .format("json")
  //    .mode(SaveMode.Overwrite)
  //    .options(Map("path" -> "src/main/resources/data/cars_dupe.json"))
  //    .save()

  /*
  JSON flags
  dateFormat as a Option
   */

  val cardDf = spark.read
    .format("json")
    .option("dateFormat", "yyyy-MM-dd") // refer this for date patterns https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")


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


  cardDf.show()
  stocksDf.show()


  // default write format is parquet do we no need to mention .format("parquet") for others you need to mention format
  // snappy compression
  stocksDf.write
    .mode(SaveMode.Overwrite)
    .save("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/stocks.parquet")

  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // reading from a database
val dfFromDb = spark.read.format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
  .option("dbtable", "public.employees")
  .option("user", "gopinathtm")
  .option("password", "buddi@tmp1")
  .option("driver", "org.postgresql.Driver")
  .load()

  dfFromDb.show()

  val moviesDf = spark.read
    .format("json")
    .option("inferSchema", value = true)
    .load("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.json")

  moviesDf.write
    .option("delimiter", "\t")
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .csv("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.tsv")

  moviesDf.write
    .mode(SaveMode.Overwrite)
    .save("/home/gopinathtm/Projects/spark-essentials/src/main/resources/data/movies.parquet")

  moviesDf.write.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("dbtable", "public.movies")
    .option("user", "gopinathtm")
    .option("password", "buddi@tmp1")
    .option("driver", "org.postgresql.Driver")
    .mode(SaveMode.Overwrite)
    .save()


}
