package analytics

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

//class FiveNumberSummary (){
object AllCities {
    def main(args: Array[String]){
    val spark = SparkSession.builder.appName("FlySTAT").getOrCreate()
    val sc = SparkContext.getOrCreate()
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    //def AtoBYearData(): Unit = {
    
        //Read in the data from HDFS as csv
        val data: DataFrame = spark.read.format("csv").option("header", "true").load("hdfs://cheyenne:30241/cs435/flySTAT/data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2018_*.csv")
        
        //Filter data so we are only looking origin airport Denver on August 8, 2014
        val filteredData: Dataset[Row] = data.filter($"OriginAirportID" === 11292).filter($"FlightDate" === "2018-11-20")
        
        //Keep only destination airport and delay minutes columns
        val relevantData: RDD[Row] = filteredData.select("DestAirportID", "DepDelayMinutes").na.drop().rdd
        
        //Take the top 25 
        val top25Dest: Array[Int] = relevantData.map( row => ( row(0).toString.toInt, 1 )).reduceByKey(_ + _).sortBy(_._2, false, 1).zipWithIndex.filter{ case (_, index) => (index < 50)}.keys.keys.collect

        //Filter relevantData to only have our top 25 destination airports
        val top25Data: RDD[Row] = relevantData.filter{ row => top25Dest.contains(row(0).toString.toInt) }
        
        //Group the sorted delays by airport
         val delaysPerAirportID: RDD[ (Int, Iterable[Double])] = top25Data.map( row => (row(0).toString.toInt, row(1).toString.toDouble) ).groupByKey().map{ case(k,v) => (k, v.toArray.sorted) }
         
         //Read in the airport names
         val airportID: RDD[(Int, String)] = spark.read.format("csv").option("header", "true").load("hdfs://cheyenne:30241/cs435/flySTAT/lookup/airportID.csv").map( row => (row(0).toString.toInt, row(1).toString) ).filter( x => top25Dest.contains(x._1) ).rdd
         
         //Join and keep the airport name instead of airport id
         val delaysPerAirport: RDD[(String, Iterable[Double] )] = delaysPerAirportID.join( airportID ).map{ case(id, (delays, name) ) => (name, delays)}
         
        //Calculate the stats
        val stats = delaysPerAirport.map { case(airport, delays) => 
                val min = delays.min
                val max = delays.max
                val Q1 = delays.slice((delays.size * .25).floor.toInt, (delays.size * .25).floor.toInt + 1).toList.head
                val median = delays.slice((delays.size * .5).floor.toInt, (delays.size * .5).floor.toInt + 1).toList.head
                val Q3 = delays.slice((delays.size * .75).floor.toInt, (delays.size * .75).floor.toInt + 1).toList.head
                val avg = delays.sum / delays.size
                val count = delays.size
                val outlierThreshold = Q3 + 1.5 * (Q3 - Q1)
                val outliers = delays.filter(x => x > outlierThreshold).mkString(" ")
                (airport, (min, max, median, Q1, Q3, avg, count, outlierThreshold, outliers ) ) }
    
        //Save to HDFS
        stats.coalesce(1).saveAsTextFile("hdfs://nashville:30841/cs435/flySTAT/statWithZeroAirport2")
        
        //Collect the same stats but this time only do delays > 0
        val nonZeroDelaysPerDate: RDD[(String, Array[Double])] = delaysPerAirport.map { case(k,v) => (k, v.filter(x => x > 0).toArray ) }
        
        val nonZeroStats = nonZeroDelaysPerDate.map { case(airport, delays) =>
                if(delays.size == 0 ){
                    (airport, (0, 0, 0, 0, 0, 0, 0, 0, "" ) )
                } else {
                    val min = delays.min
                    val max = delays.max
                    val Q1 = delays( (delays.size * .25).floor.toInt)
                    val median = delays( (delays.size * .5).floor.toInt)
                    val Q3 = delays( (delays.size * .75).floor.toInt)
                    val avg = delays.sum / delays.size
                    val count = delays.size
                    val outlierThreshold = Q3 + 1.5 * (Q3 - Q1)
                    val outliers = delays.filter(x => x > outlierThreshold).mkString(" ")
                    (airport, (min, max, median, Q1, Q3, avg, count, outlierThreshold, outliers ) )
                    }
                }
                
        nonZeroStats.coalesce(1).saveAsTextFile("hdfs://nashville:30841/cs435/flySTAT/statWithoutZeroAirport2")
        
    }
}
