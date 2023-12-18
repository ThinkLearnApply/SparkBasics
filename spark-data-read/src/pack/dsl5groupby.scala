package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object dsl5groupby {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop") // Put your drive accordingly

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true")
      .load("file:///D:/data/agg.csv")

    df.show()

    val finaldf = df.groupBy("name").agg(sum("amt").cast(IntegerType).as("total"), count("amt").as("cnt"))
    finaldf.show()

  }

}

