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
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object LinearRegression {
    def main(args: Array[String]){
        //args(0) = date in format MM-DD
        //args(1) = output file name
    
        //Create spark SparkSession and spark context
        val spark = SparkSession.builder.appName("flySTAT LinearRegression").getOrCreate()
        val sc = SparkContext.getOrCreate()
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
                
        //Raw data
        val data: DataFrame = spark.read.format("csv").option("header", "true").load("hdfs://cheyenne:30241/cs435/flySTAT/data/*.csv")
        
        //Get data for single airport
        val singleAirportData: Dataset[Row] = data.filter($"OriginAirportID" === 11292)
        
        //Drop all columns except date and delay
        val singleAirportRelevantData: RDD[Row] = singleAirportData.select("FlightDate", "DepTime", "DepDelayMinutes").na.drop().rdd
        
        //Filter out all dates that are not in 2009-2018 on the specific date
        val singleDaySingleAirportData: RDD[Row] = singleAirportRelevantData.filter{row => row(0).toString.substring(0,4) != "2019" && row(0).toString.substring(5) == args(0)}
        
        //Reformat data so date & time is timestamp as long     
        val finalData: RDD[(Double, Double)] = singleDaySingleAirportData.map(row => (row(2).toString.toDouble, (row(0).toString.replace("-", "") + row(1).toString).toDouble))
        
        //Reformating testing data into the correct format
        val parsedData: RDD[LabeledPoint] = finalData.map{ case(delay, timestamp) =>
            LabeledPoint(delay, Vectors.dense(scala.math.log(timestamp)))
        }.cache()
        
        //Set variables for training
        val numIterations = 100
        val stepSize = 0.00000001
        
        //Train the model with data from 2009 - 2018
        val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)
        
        val coefficients = model.weights
        val intercept = model.intercept
    }
}
