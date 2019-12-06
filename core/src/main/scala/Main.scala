import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.ArrayBuffer

import analytics.DelayStats
import analytics.AirportRank

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length >=  2) {
      val inputDirectory : String = args(0)
      val outputDirectory: String = args(1)
      
      var fileNames: ArrayBuffer[String] = getFileNames(inputDirectory) 
      var delayStats = new DelayStats(fileNames, "flySTAT", inputDirectory, outputDirectory)
      delayStats.averageDelays()
    }
    else {
      printUsageMessage()
    }
  }

  /**
   * Input: A String containing the HDFS directory name.
   * Output: ArrayBuffer[String] containing all the files in the HDFS directory.
   *
   * Example found at: https://stackoverflow.com/questions/33394884/spark-scala-list-folders-in-directory 
   */
  def getFileNames(directory: String): ArrayBuffer[String] = {
    var fileNames: ArrayBuffer[String] = ArrayBuffer[String]()
      
    val fs = FileSystem.get(new Configuration())
    val status = fs.listStatus(new Path(directory))
    status.foreach( fileStatus => fileNames += (directory + "/" + fileStatus.getPath().getName()) )
    return fileNames
  }


  def printUsageMessage(): Unit = {
    println("\nUsage:\n\n" +
            "$\t${SPARK_HOME}/bin/spark-submit  \\\n" +
            "\t\t--class Main \\\n" +
            "\t\t--master <spark_master>  \\\n" +
            "\t\t--supervise <jar_file> <hdfs_input_dir> <hdfs_output_dir>\n")
    }


}
