package analytics

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.mllib.regression.LabeledPoint
import  org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object LinearRegression {
    def main(args: Array[String]){
        //args(0) = date in format MM-DD
        //args(1) = output file name
        //args(2) = airport ID (denver = 11292)
    
        //Create spark SparkSession and spark context
        val spark = SparkSession.builder.appName("flySTAT LinearRegression").getOrCreate()
        val sc = SparkContext.getOrCreate()
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
                
        //Raw data
        val data: DataFrame = spark.read.format("csv").option("header", "true").load("hdfs://cheyenne:30241/cs435/flySTAT/data/*.csv")
        
        //Get data for single airport
        val singleAirportData: Dataset[Row] = data.filter($"OriginAirportID" === args(2))
        
        //Drop all columns except date and delay
        val singleAirportRelevantData: RDD[Row] = singleAirportData.select("FlightDate", "DepTime", "DepDelayMinutes").na.drop().rdd
        
        
//      ------------------- WITHOUT ZEROS -------------------
        
        
        //Filter out all dates that are not in 2009-2018 on the specific date
        val singleDaySingleAirportWithoutZeros: RDD[Row] = singleAirportRelevantData.filter{row => row(0).toString.substring(0,4) != "2019" && row(0).toString.substring(5) == args(0) && row(2).toString.toDouble > 0.0}
        
        //Reformat data so date & time is timestamp as long     
        val finalDataWithoutZeros: RDD[(Double, Double)] = singleDaySingleAirportWithoutZeros.map(row => (row(2).toString.toDouble, row(0).toString.substring(2,4).toDouble, row(1).toString.toDouble * 3.5/10000) ).map( x => (x._1, x._2 + x._3) )
        
        //Reformating testing data into the correct format
        val parsedDataWithoutZeros: RDD[LabeledPoint] = finalDataWithoutZeros.map{ case(delay, timestamp) =>
            LabeledPoint(delay, Vectors.dense(timestamp))
        }.cache()
        
        //Set variables for training
        val numIterations = 100
        val stepSize = 0.001
        
        //Train the model with data from 2009 - 2018
        val modelWithoutZeros = LinearRegressionWithSGD.train(parsedDataWithoutZeros, numIterations, stepSize)
        
        //We are using 19.42 as a timestamp for a flight in 2019 at noon
        val statsWithoutZeros = Array(args(0), (modelWithoutZeros.weights(0) * 19.42 + modelWithoutZeros.intercept).toString)
                
        
//      ------------------- WITH ZEROS -------------------


        //Filter out all dates that are not in 2009-2018 on the specific date
        val singleDaySingleAirportWithZeros: RDD[Row] = singleAirportRelevantData.filter{row => row(0).toString.substring(0,4) != "2019" && row(0).toString.substring(5) == args(0)}
        
        //Reformat data so date & time is timestamp as long     
        val finalDataWithZeros: RDD[(Double, Double)] = singleDaySingleAirportWithZeros.map(row => (row(2).toString.toDouble, row(0).toString.substring(2,4).toDouble, row(1).toString.toDouble * 3.5/10000) ).map( x => (x._1, x._2 + x._3) )
        
        //Reformating testing data into the correct format
        val parsedDataWithZeros: RDD[LabeledPoint] = finalDataWithZeros.map{ case(delay, timestamp) =>
            LabeledPoint(delay, Vectors.dense(timestamp))
        }.cache()
        
        //Train the model with data from 2009 - 2018
        val modelWithZeros = LinearRegressionWithSGD.train(parsedDataWithZeros, numIterations, stepSize)
        
        //We are using 19.42 as a timestamp for a flight in 2019 at noon
        val statsWithZeros = Array(args(0), (modelWithZeros.weights(0) * 19.42 + modelWithZeros.intercept).toString)
                        
        
//      ------------------- OUTPUT DATA -------------------


        sc.parallelize(statsWithoutZeros).coalesce(1).saveAsTextFile(args(1) + "/" + args(0) + "/withoutZerosStats")
        sc.parallelize(statsWithZeros).coalesce(1).saveAsTextFile(args(1) + "/" + args(0) + "/withZerosStats")    
    }
}
