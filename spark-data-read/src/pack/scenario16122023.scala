package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object scenario16122023 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop") // path of winutils.exe

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val d1df = spark.read.format("csv").option("header", "true").load("file:///D:/data/d1.csv")
    d1df.show()

    val d2df = spark.read.format("csv").option("header", "true").load("file:///D:/data/d2.csv")
    d2df.show()

    val d3df = spark.read.format("csv").option("header", "true").load("file:///D:/data/d3.csv")
    d3df.show()

    val joinedDf = d1df.join(d2df, Seq("id"), "left")
      .join(d3df, Seq("id"), "left")
    joinedDf.show()

    val notnullDf = joinedDf.withColumn("salary", expr("coalesce(salary,0)"))
      .withColumn("salary1", expr("coalesce(salary1,0)"))
    notnullDf.show()

    val sumdf = notnullDf.withColumn("salary", expr("salary+salary1"))
    sumdf.show()

    val resultdf = sumdf.selectExpr("id", "name", "cast(salary as int) as salary")
    resultdf.show()
  }

}