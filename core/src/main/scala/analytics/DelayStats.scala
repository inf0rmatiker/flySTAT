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
  
  def predictDelays(): Unit = {
    
    val files: DataFrame = spark.read.format("csv")
                          .option("header", "true")
                          .load("hdfs://cheyenne:30241/cs435/flySTAT/data/*.csv")
    
    val relevantData: RDD[Row] = files.select("OriginAirportID", "FlightDate", "DepDelayMinutes", "ArrDelayMinutes",  "DestAirportID", "DepDel15").na.drop().rdd
    
    // In the form ( (OriginAirportID, FlightDate), [FlightDate, DepDelayMinutes, ArrDelayMinutes, DestAirportID, DepDel15] )
    val formattedData: RDD[((String,String), Array[String])] = relevantData.map(row => 
                                                                    ((row(0).toString,row(1).toString),
                                                                     Array(row(1).toString,row(2).toString,row(3).toString,row(4).toString,row(5).toString) ))
    
    // In the form ( (OriginAirportID, FlightDate), Iterable([FlightDate, DepDelayMinutes, ArrDelayMinutes, DestAirportID, DepDel15], [...], ...) )
    var groupedByKey: RDD[((String,String), Iterable[Array[String]])] = formattedData.groupByKey()
    
    // In the form ( (OriginAirportID, FlightDate), [delayedMinutesSum, totalCount, delayedCount] )
    var dailyAverageDelays: RDD[((String,String), Array[Double])] = groupedByKey.map{ case(key, values) =>
      var delayedMinutesSum, delayedCount, totalCount: Double = 0.0
      values.foreach{ valueArray =>
        totalCount += 1
        if (valueArray(1).toDouble > 0.0) {
          delayedCount += 1
          delayedMinutesSum += valueArray(1).toDouble
        }
      }
      (key, Array(delayedMinutesSum, totalCount, delayedCount))
    }

    // In the form ( (OriginAirportID,MM-DD), [delayMinutesSum, totalCount, delayedCount, localTotalAverage, localDelayedAverage, 1.0] )
    var formattedAverages: RDD[((String,String), Array[Double])] = dailyAverageDelays.map{ case((airportID,date), values) =>
                                                          val totalAverage:   Double = if (values(1) > 0.0) values(0)/values(1) else 0.0
                                                          val delayedAverage: Double = if (values(2) > 0.0) values(0)/values(2) else 0.0
                                                          ((airportID,date.substring(5, date.length())), 
                                                            Array(values(0), values(1), values(2), totalAverage, delayedAverage, 1.0)) }

    // Reduces all the values by key by summing them 
    // In the form ( (OriginAirportID,MM-DD), [delayMinutesSum, totalCount, delayedCount, globalTotalAverage, globalDelayedAverage, daysConsidered] )
    var yearlyAverages: RDD[((String,String), List[Double])] = formattedAverages.reduceByKey{ (x,y) =>
                                                                    for (i <- 0 until x.length) {
                                                                      y(i) += x(i)
                                                                    }
                                                                    y
                                                                }.mapValues{ values => 
                                                                  values(3) /= values(5)
                                                                  values(4) /= values(5)
                                                                  values.toList }
                                                         
    implicit val sortTuplesByStrings = new Ordering[(String,String)] {
      override def compare(a: (String, String), b: (String, String)): Int = {
        if (a._1.compare(b._1) < 0) return -1
        else if (a._1.compare(b._1) > 0) return 1
        else {
          return a._2.compare(b._2)
        }
      }
    }
    
    // In the form ( (OriginAirportID,MM-DD), [delayMinutesSum, totalCount, delayedCount, globalTotalAverage, globalDelayedAverage, daysConsidered, percentFlightsDelayed] )
    val sortedDifferences = yearlyAverages.sortByKey().mapValues{ case(valueDoubles) =>
                                                                   var finalStats = valueDoubles :+ valueDoubles(2)/valueDoubles(1) * 100.0 // Percentage delayed 
                                                                   (finalStats.collect{ case value: Double => f"$value%.2f" }) 
                                                                 }
    sortedDifferences.saveAsTextFile(outputDirectory)
  }

    
  def getAirportCount(records: RDD[Array[String]] ): Int = {
    return 0
  }

}
