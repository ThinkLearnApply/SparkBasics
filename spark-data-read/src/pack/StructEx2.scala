package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StructEx2 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop") // Put your drive accordingly

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val jsondf = spark.read.format("json").option("multiline", "true").load("file:///D:/data/c2.json")

    jsondf.show()
    jsondf.printSchema()

    val flatdf = jsondf.withColumn("permanentAddress", expr("zeyoaddress.user.permanentAddress"))
      .withColumn("temporaryAddress", expr("zeyoaddress.user.temporaryAddress"))
      .drop("zeyoaddress")

    flatdf.show()
    flatdf.printSchema()

  }
}