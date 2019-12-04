package analytics

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class DelayStats (inputFileNames: ArrayBuffer[String], appName: String, inputDirectory: String, outputDirectory: String) {
  val spark = SparkSession.builder.appName("FlySTAT").getOrCreate()
  val sc = SparkContext.getOrCreate()
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  
  def averageDelays(): Unit = {
    
    val files: DataFrame = spark.read.format("csv")
                          .option("header", "true")
                          .load("hdfs://cheyenne:30241/cs435/flySTAT/data/*.csv")
    
    val relevantData:  RDD[Row] = files.select("OriginAirportID", "FlightDate", "DepDelayMinutes", "ArrDelayMinutes",  "DestAirportID").na.drop().rdd
    
    // In the form ( (OriginAirportID, FlightDate), [FlightDate, DepDelayMinutes, ArrDelayMinutes, DestAirportID] )
    val formattedData: RDD[((String,String), Array[String])] = relevantData.map(row => 
                                                                    ((row(0).toString,row(1).toString),
                                                                     Array(row(1).toString,row(2).toString,row(3).toString,row(4).toString) ))
    
    // Filter out any records for 2019, to test against
    val subset: RDD[((String,String), Array[String])] = formattedData.filter( record =>
                                                              record._1._2.contains("2019") )
    
    // In the form ( (OriginAirportID, FlightDate), Iterable([FlightDate, DepDelayMinutes, ArrDelayMinutes, DestAirportID], [...], ...) )
    var groupedByKey: RDD[((String,String), Iterable[Array[String]])] = subset.groupByKey()
    
    // In the form ( (OriginAirportID, FlightDate), AverageDelayMinutes )
    var dailyAverageDelays: RDD[((String,String), Double)] = groupedByKey.map{ case(key, values) =>
      var count: Int = 0
      var totalSum: Double = 0.0
      
      values.foreach{ valueArray =>
        count += 1
        totalSum += valueArray(1).toDouble
      }
      (key, totalSum / count)
    }
    
    var yearlyAveragesByDay: RDD[(String, Iterable[(String, Double)])] = dailyAverageDelays.map{ case((airportID,date), averageDelay) =>
                                                          ((airportID,date.substring(5, date.length())), averageDelay) }
                                                        .mapValues((_,1)) // Add a "1" to the value tuples for counting
                                                        .reduceByKey( (first,second) => (first._1 + second._1, first._2 + second._2)) // Sum delays and counts
                                                        .mapValues{ case (sum, count) => (1.0 * sum) / count } // Transform to delay averages
                                                        .map( record => (record._1._2, (record._1._1,record._2)) ) // Make the month and day the key
                                                        .groupByKey().sortByKey()
    
  }

    
  def getAirportCount(records: RDD[Array[String]] ): Int = {
    return 0
  }

}
