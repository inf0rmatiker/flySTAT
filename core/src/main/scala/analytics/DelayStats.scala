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
    val formattedData: RDD[((String,String), Array[String])] = relevantData.map(row => 
                                                                            ( (row(0).toString, 
                                                                              Array(row(1).toString,row(2).toString,row(3).toString,row(4).toString) )
                                                                      )
    
 
    for (year <- 2009 to 2009) {
      var filteredByDate  = formattedData.filter(record => record._2(0).contains(year + "-01-01"))
      var groupedByID     = filteredByDate.groupByKey()
      
      var groupedByDate = groupedByID.map{ case(id, values) =>
        var count: Int = 0
        var totalSum: Double = 0.0
        
        tuples.foreach{ valueArray =>
          
        }
      }
      janFirstAverages.coalesce(1).saveAsTextFile(outputDirectory)
  
    }
  }

    
  def getAirportCount(records: RDD[Array[String]] ): Int = {
    return 0
  }

}
