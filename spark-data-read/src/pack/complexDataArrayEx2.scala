package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexDataArrayEx2 {
  
	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // put your path

			val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			val petsjsondf = spark
			              .read
			              .format("json")
			              .option("multiline","true")
			              .load("file:///D:/data/pets1.json")
			              
			petsjsondf.show()
			petsjsondf.printSchema()
			
			
			println("***** In order to flatened the Json array we can use both explode(elementName) ")
			
			
			val flateneddf = petsjsondf.withColumn("Pets", expr("explode(Pets)"))
			flateneddf.show()
			flateneddf.printSchema()

	}

}
