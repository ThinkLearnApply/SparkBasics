
package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object awsIntegration {


	def main(args:Array[String]):Unit={



			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly

			val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)   // RDD

			sc.setLogLevel("ERROR")

			val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

			import spark.implicits._

      
			val awsdf = spark
			            .read
			            .format("json")
			            .option("fs.s3a.access.key","AKIAYYWDIYCZ74LHALFC")
			            .option("fs.s3a.secret.key","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")
			            .load("s3a://zeyotics/devices.json")
			
			
			
			awsdf.show()
			
			
			
			
			
			

	}

}