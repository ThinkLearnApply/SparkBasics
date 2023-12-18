

package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object dslTask {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop") // Put your drive accordingly

    val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").
                    set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf) // RDD

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate() //Dataframe

    import spark.implicits._
    val collist = List("", "", "", "")

    val targetdf = spark.read.format("csv").option("header", "true")
      .load("file:///D:/data/target.csv")

    targetdf.show()

    val sourcedf = spark.read.format("csv").option("header", "true")
      .load("file:///D:/data/source.csv")

    sourcedf.show()

    val outer = sourcedf.join(targetdf, Seq("id"), "full").orderBy("id")
    outer.show()

    val commentDf = outer.withColumn("comment", 
        expr("case when sname=tname then 'Match' else 'Mismatch' end"))
    commentDf.show()

    val mismatchDf = commentDf.filter(!(col("comment") === "Match"))
    mismatchDf.show()

    val prefinalDf = mismatchDf.withColumn("comment", 
    expr("case when sname is null then 'New in Target' when tname is null then 'New in Source' else comment end"))
    prefinalDf.show()

    val finaldf = prefinalDf.select("id", "comment").orderBy(desc("comment"))
    finaldf.show()

  }

}
