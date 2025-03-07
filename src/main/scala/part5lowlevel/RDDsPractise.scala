package part5lowlevel

import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object RDDsPractise extends App {

  val spark = SparkSession.builder()
    .appName("SQL Practise")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  // Read text file and create an RDD
  val rdd = sc.textFile("/home/gopinathtm/Downloads/tmp/words")
  val listString = rdd.map(_.split(' ')) //.collect().foreach(println)

  val txt = spark.read.text("/home/gopinathtm/Downloads/tmp/words")

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(filename: String) = {
    val source = Source.fromFile(filename)
    val stockValue = source.getLines()
    val stockValues = source.getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
    source.close()
    stockValues
  }

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv", 2).map { arrs =>
    val arr = arrs.split(',')
    Row(arr(0), arr(1), arr(2))
  }


  val add = (a: Int, b: Int) => a + b

  val stocksRDD = sc.textFile("src/main/resources/data/stocks.csv", 2).flatMap(_.toLowerCase.split(' '))
  val groupedWords = stocksRDD.map(x => (x, 1)).groupByKey().map(kv => (kv._1, kv._2.reduce(add)))

  //  stocksRDD2.map(_.)
  //  val stocksRDD3 = stocksRDD2
  //    .map(line => line.split(","))
  //    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
  //    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble)).toDF()

  //  stocksRDD2.toDF("symbol", "date", "price").show()
  // stocksRDD3.show()


}
