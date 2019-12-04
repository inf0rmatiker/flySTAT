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
object FiveNumberSummary {
    def main(args: Array[String]){
    val spark = SparkSession.builder.appName("FlySTAT").getOrCreate()
    val sc = SparkContext.getOrCreate()
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    //def AtoBYearData(): Unit = {
        //Read in the data from HDFS as csv
        val data: DataFrame = spark.read.format("csv").option("header", "true").load("hdfs://cheyenne:30241/cs435/flySTAT/data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2018_*.csv")
        
        //Filter data so we are only looking origin and destination airports Denver and Chicago currently
        val filteredData: Dataset[Row] = data.filter($"OriginAirportID" === 11292).filter($"DestAirportID" === 13930)
        
        //We only need Origin Airport, Destination Airport, Date, Delayed Minutes 
        val relevantData: RDD[Row] = filteredData.select("FlightDate", "DepDelayMinutes").na.drop().rdd
        
        //Group the sorted delays by date
        val delaysPerDate: RDD[ (String, Iterable[Double])] = relevantData.map( row => (row(0).toString, row(1).toString.toDouble) ).groupByKey().map{ case(k,v) => (k, v.toArray.sorted) }
        
        delaysPerDate.map{ case(k,v) => (k, v.mkString(" ") ) }.coalesce(1).saveAsTextFile("hdfs://nashville:30841/cs435/flySTAT/data")

        //Calculate the stats
        val stats = delaysPerDate.map { case(date, delays) => 
                val min = delays.min
                val max = delays.max
                val Q1 = delays.slice((delays.size * .25).floor.toInt, (delays.size * .25).floor.toInt + 1).toList.head
                val median = delays.slice((delays.size * .5).floor.toInt, (delays.size * .5).floor.toInt + 1).toList.head
                val Q3 = delays.slice((delays.size * .75).floor.toInt, (delays.size * .75).floor.toInt + 1).toList.head
                val avg = delays.sum / delays.size
                val count = delays.size
                val outlierThreshold = Q3 + 1.5 * (Q3 - Q1)
                val outliers = delays.filter(x => x > outlierThreshold).mkString(" ")
                (date, (min, max, median, Q1, Q3, avg, count, outliers, outlierThreshold ) ) }
    
        //Save to HDFS
        stats.coalesce(1).saveAsTextFile("hdfs://nashville:30841/cs435/flySTAT/statWithZero")
        
        //Collect the same stats but this time only do delays > 0
        val nonZeroDelaysPerDate: RDD[(String, Array[Double])] = delaysPerDate.map { case(k,v) => (k, v.filter(x => x > 0).toArray ) }.filter{ case(k, v) => v.size != 0 }
        
        nonZeroDelaysPerDate.map{ case(k,v) => (k, v.mkString(" ")) }.coalesce(1).saveAsTextFile("hdfs://nashville:30841/cs435/flySTAT/data2")

        
        val nonZeroStats = nonZeroDelaysPerDate.map { case(date, delays) => 
                val min = delays.min
                val max = delays.max
                val Q1 = delays( (delays.size * .25).floor.toInt)
                val median = delays( (delays.size * .5).floor.toInt)
                val Q3 = delays( (delays.size * .75).floor.toInt)
                val avg = delays.sum / delays.size
                val count = delays.size
                val outlierThreshold = Q3 + 1.5 * (Q3 - Q1)
                val outliers = delays.filter(x => x > outlierThreshold).mkString(" ")
                (date, (min, max, median, Q1, Q3, avg, count, outliers ) ) }
                
        nonZeroStats.coalesce(1).saveAsTextFile("hdfs://nashville:30841/cs435/flySTAT/statWithoutZero")
        
    }
}
