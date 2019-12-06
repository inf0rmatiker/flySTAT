package analytics

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import java.lang.Double

class AirportRank (inputFileNames: ArrayBuffer[String], appName: String, inputDirectory: String, outputDirectory: String) {
  val spark = SparkSession.builder.appName("FlySTAT").getOrCreate()
  val sc = SparkContext.getOrCreate()
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  
  def calculateAirportRanks(): Unit = {
    
    val files = spark.read.format("csv")
                          .option("header", "true")
                          .load("hdfs://cheyenne:30241/cs435/flySTAT/data/*.csv")

    
  }

    
  def getAirportCount(records: RDD[Array[String]] ): Int = {
    return 0
  }

}
