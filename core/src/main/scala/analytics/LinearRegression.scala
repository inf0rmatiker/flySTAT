package analytics

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
//import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.sql.DataFrame


import scala.collection.mutable.ArrayBuffer
import java.lang.Double

object LinearRegression {
    def main(args: Array[String]){
        //args(0) = date in format MM-DD
        //args(1) = output file name
    
        //Create spark SparkSession and spark context
        val spark = SparkSession.builder.appName("flySTAT LinearRegression").getOrCreate()
        val sc = SparkContext.getOrCreate()
        
        //Raw data
        val files: DataFrame = spark.read.format("csv").option("header", "true").load("hdfs://cheyenne:30241/cs435/flySTAT/data/*.csv")
        
        //drop irrelevant data
        val relevantData:  RDD[Row] = files.select("OriginAirportID", "FlightDate", "DepDelayMinutes").na.drop().rdd
        
        // In the form ( (OriginAirportID, FlightDate), [DepDelayMinutes, DestAirportID] )
        val formattedData: RDD[((String,String), String)] = relevantData.map(row => ((row(0).toString,row(1).toString), row(2).toString))
        
        //Get single day throughout the years (excluding 2019) for all airports
        val singleDayData :RDD[((String, String), String)] = formattedData.filter{case((airport, date), delay) => date.substring(0,4) != "2019" & date.substring(5) == args(0)}
        
        val groupedSingleDayData :RDD[((String, String), Iterable[String])] = singleDayData.groupByKey()
        
        //Create linear regression 
//         val glr = new GeneralizedLinearRegression().setFamily("gaussian").setLink("identity").setMaxIter(20).setRegParam(0.3)

        singleDayData.saveAsTextFile(args(1))

    }
}
