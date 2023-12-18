package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object xmlRead {
  
 	def main(args:Array[String]):Unit={


			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly

			val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)   // RDD

			sc.setLogLevel("ERROR")

			val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

			import spark.implicits._
			
			val xmldf = spark.read.format("xml").option("rowtag","book").load("file:///D:/data/book.xml")
			xmldf.show()
			
			val cdf = spark.read.format("csv").option("header","true").load("file:///D:/data/allc.csv")
			cdf.show()

			cdf.write.format("csv").partitionBy("country").mode("overwrite").save("file:///D:/data/partitionedData")
			cdf.write.format("csv").partitionBy("country","chk").mode("overwrite").save("file:///D:/data/partitionedData2")
			
			cdf.write.format("csv").partitionBy("country","chk").mode("append").save("file:///D:/data/partitionedData2")

						cdf.write.format("csv").partitionBy("country","chk").mode("ignore").save("file:///D:/data/partitionedData2")
 	}
}