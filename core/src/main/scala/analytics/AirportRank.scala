package analytics

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

class AirportRank (inputFileNames: ArrayBuffer[String], appName: String, inputDirectory: String, outputDirectory: String) {
  var conf: SparkConf = new SparkConf().setAppName(appName)
  var sc: SparkContext = new SparkContext(conf)

  def calculateAirportRanks(): Unit = {
    val lineCount: Long = getAirportCount()
    sc.parallelize(Seq(lineCount), 1).saveAsTextFile(outputDirectory)
  }

    
  def getAirportCount(): Long =  {
    var totalCount: Long = 0
    val files = sc.textFile(inputDirectory)
    totalCount += files.count()
    return totalCount
  }

}
