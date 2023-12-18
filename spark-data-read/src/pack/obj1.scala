package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object obj1 {

  def main(args: Array[String]): Unit = {

    val structschema = StructType(Array(
      StructField("id", StringType, true),
      StructField("tdate", StringType, true),
      StructField("amount", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true),
      StructField("standby", StringType, true)))

    System.setProperty("hadoop.home.dir", "D:\\hadoop") // Put your drive accordingly

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true")
      .load("file:///D:/data/dt.txt")

    df.show()

    val seldf = df.select("id", "category")

    seldf.show()

    val dropdf = df.drop("product")

    dropdf.show()

    val dtnew = spark.read.format("csv").option("header", "true")
      .load("file:///D:/data/dtnew.txt")

    println("=========== dtnew.txt ===========")
    dtnew.show()

    val filcat = dtnew.filter(col("category") === "Exercise")
    filcat.show()

    val multiColumnAndFilter = dtnew.filter(
      col("category") === "Exercise"
        &&
        col("spendby") === "cash")

    multiColumnAndFilter.show()
    
     val multiColumnAndFilter1 = dtnew.filter(
      col("category") === "Gymnastics"
        &&
        col("spendby") === "cash")

    multiColumnAndFilter1.show()
    
    

    val multiColumnOrFilter = dtnew.filter(
      col("category") === "Exercise"
        ||
        col("spendby") === "cash")

    multiColumnOrFilter.show()
    
    val multiColumnOrFilter1 = dtnew.filter(
      col("category") === "Gymnastics"
        ||
        col("spendby") === "credit")

    multiColumnOrFilter1.show()

    val multiValuefilcat = dtnew.filter(
      col("category") isin ("Exercise", "Team Sports"))

    multiValuefilcat.show()

    val tdatefilcat = dtnew.filter(
      col("tdate") > "06-26-2011")

    tdatefilcat.show()

    val amountmorethanfilcat = dtnew.filter(
      col("amount") > 201)

    amountmorethanfilcat.show()

      val productcontainsfilcat = dtnew.filter(
      col("product") like ("%Gymnastics%"))

    productcontainsfilcat.show()
    
    val notnullproductfilcat = dtnew.filter(
      col("product") isNotNull)

    notnullproductfilcat.show()

  

     val nullproductfilcat = dtnew.filter(
      col("product") isNull)

    nullproductfilcat.show()
     val notfilcat = dtnew.filter(!(col("category") === "Exercise"))
    notfilcat.show()
    
  }

}