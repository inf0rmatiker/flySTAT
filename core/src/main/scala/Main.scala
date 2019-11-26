import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Main {

  def main(args: Array[String]): Unit = {
    var conf: SparkConf = new SparkConf().setAppName("flySTAT")
    var sc: SparkContext = new SparkContext(conf)

    
    println("Hello, World")


  }


}
