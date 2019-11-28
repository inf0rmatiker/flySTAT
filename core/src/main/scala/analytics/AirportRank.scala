package analytics

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import java.lang.Double

class AirportRank (inputFileNames: ArrayBuffer[String], appName: String, inputDirectory: String, outputDirectory: String) {
  var conf: SparkConf = new SparkConf().setAppName(appName)
  var sc: SparkContext = new SparkContext(conf)

  val files = sc.textFile(inputDirectory)

  def calculateAirportRanks(): Unit = {
    var records    = files.filter(line => !line.contains("DOT_ID_Reporting_Airline")).map(line => line.split(","))
    var airportIds = records.map(record => record(11)).distinct
    var airportCount = airportIds.count().toInt

    var ranks: Array[Double] = Array.fill(airportCount){ 1.0 / airportCount }
    sc.parallelize(ranks, 1).saveAsTextFile(outputDirectory)
  }

    
  def getAirportCount(): Long =  {
    var totalCount: Long = 0
    val files = sc.textFile(inputDirectory)
    totalCount += files.count()
    return totalCount
  }

}
