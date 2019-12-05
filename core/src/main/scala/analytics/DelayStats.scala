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
    
    val relevantData:  RDD[Row] = files.select("OriginAirportID", "FlightDate", "DepDelayMinutes", "ArrDelayMinutes",  "DestAirportID", "DepDel15").na.drop().rdd
    
    // In the form ( (OriginAirportID, FlightDate), [FlightDate, DepDelayMinutes, ArrDelayMinutes, DestAirportID, DepDel15] )
    val formattedData: RDD[((String,String), Array[String])] = relevantData.map(row => 
                                                                    ((row(0).toString,row(1).toString),
                                                                     Array(row(1).toString,row(2).toString,row(3).toString,row(4).toString,row(5).toString) ))
    
    // Filter out any records for 2019, to test against
    val subset: RDD[((String,String), Array[String])] = formattedData.filter( record =>
                                                              !record._1._2.contains("2019") )
    
    // In the form ( (OriginAirportID, FlightDate), Iterable([FlightDate, DepDelayMinutes, ArrDelayMinutes, DestAirportID, DepDel15], [...], ...) )
    var groupedByKey: RDD[((String,String), Iterable[Array[String]])] = subset.groupByKey()
    
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

/*    
    var yearlyAveragesByDay: RDD[(String, Iterable[(String, Double)])] = dailyAverageDelays.map{ case((airportID,date), averageDelay) =>
                                                          ((airportID,date.substring(5, date.length())), averageDelay) }
                                                        .mapValues((_,1)) // Add a "1" to the value tuples for counting
                                                        .reduceByKey( (first,second) => (first._1 + second._1, first._2 + second._2)) // Sum delays and counts
                                                        .mapValues{ case (sum, count) => (1.0 * sum) / count } // Transform to delay averages
                                                        .map( record => (record._1._2, (record._1._1,record._2)) ) // Make the month and day the key
                                                        .groupByKey().sortByKey()


*/

    // In the form ( (OriginAirportID,MM-DD), [delayMinutesSum, totalCount, delayedCount, totalAverage, delayedAverage, 1.0] )
    var formattedAverages: RDD[((String,String), Array[Double])] = dailyAverageDelays.map{ case((airportID,date), values) =>
                                                          val totalAverage:   Double = if (values(1) > 0.0) values(0)/values(1) else 0.0
                                                          val delayedAverage: Double = if (values(2) > 0.0) values(0)/values(2) else 0.0
                                                          ((airportID,date.substring(5, date.length())), 
                                                            Array(values(0), values(1), values(2), totalAverage, delayedAverage, 1.0)) }

    // Reduces all the values by key by summing them 
    var yearlyAverages = formattedAverages.reduceByKey{ (x,y) =>
                                                        for (i <- 0 until x.length) {
                                                          y(i) += x(i)
                                                        }
                                                        y
                                                      }.mapValues{ values => 
                                                                    values(3) /= values(5)
                                                                    values(4) /= values(5)
                                                                    values.toList }
                                                       
    // yearlyAverages.saveAsTextFile(outputDirectory)


    // ========== Comparison ===========    

    var groundSet: RDD[((String,String), Array[String])] = formattedData.filter( record =>
                                                                record._1._2.contains("2019") )

    var groupedByKey2019: RDD[((String,String), Iterable[Array[String]])] = groundSet.groupByKey() 
    var dailyAverageDelays2019: RDD[((String,String), List[Double])] = groupedByKey2019.map{ case(key, values) =>
      var delayedMinuteSum, delayedCount, totalCount: Double = 0.0
      values.foreach{ valueArray =>
        totalCount += 1
        if (valueArray(1).toDouble > 1) {
          delayedCount += 1
          delayedMinuteSum += valueArray(1).toDouble
        }
      }
      (key, Array(delayedMinuteSum, totalCount, delayedCount))
    }.map{ case((airportID,date), values) =>
          val totalAverage:   Double = if (values(1) > 0.0) values(0)/values(1) else 0.0
          val delayedAverage: Double = if (values(2) > 0.0) values(0)/values(2) else 0.0
          ((airportID,date.substring(5, date.length())), 
          Array(values(0), values(1), values(2), totalAverage, delayedAverage, 1.0)) 
    }.mapValues{ values => values.toList }


    var differences = yearlyAverages.join(dailyAverageDelays2019).mapValues{ case(first,second) => (first,second,first(4)-second(4)) } 
    var filteredDifferences = differences.filter{ case((airportID,date),(predicted,actual,differences)) =>
      airportID == "11292"
    }

    implicit val sortTuplesByStrings = new Ordering[(String,String)] {
      override def compare(a: (String, String), b: (String, String)): Int = {
        if (a._1.compare(b._1) < 0) return -1
        else if (a._1.compare(b._1) > 0) return 1
        else {
          return a._2.compare(b._2)
        }
      }
    }

    val sortedDifferences = filteredDifferences.sortByKey().coalesce(1).saveAsTextFile(outputDirectory)

/*    

    var differences = yearlyAveragesByDay.join(dailyAverageDelays2019).mapValues{ case(first,second) => 
                                                                                  val difference = first - second
                                                                                  (f"$first%.2f", f"$second%.2f", f"$difference%.2f") }
    

    var sortedDifferences = differences.sortByKey()
    sortedDifferences.saveAsTextFile(outputDirectory)
*/
  }

    
  def getAirportCount(records: RDD[Array[String]] ): Int = {
    return 0
  }

}
