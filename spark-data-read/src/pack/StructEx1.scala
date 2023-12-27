package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StructEx1 {
  
 	def main(args:Array[String]):Unit={


			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly

			val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)   // RDD

			sc.setLogLevel("ERROR")

			val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

			import spark.implicits._
			
			val jsondf = spark.read.format("json").option("multiline","true").load("file:///D:/data/pic.json")
			jsondf.show()
			
			jsondf.printSchema()
			
			
		val flatdf = jsondf.select(
			                            "id",
			                            "image.height",
			                            "image.url",
			                            "image.width",
			                            "name",
			                            "type"
			)
			
			
			flatdf.show()
			flatdf.printSchema()

		
      val complexdf = flatdf.select(
			                            col("id"),
			                            struct(
			                              col("height"),
			                              col("url"),
			                              col("width")
			                                ).as("details"),
			                            struct(
			                                  col("name"),
			                                  col("type")
			                                ).as("otherdetails")
			)
		
			complexdf.show()
			complexdf.printSchema()
			
			
			complexdf.write.format("json").save("file:///D:/data/compdata")

 	}
}