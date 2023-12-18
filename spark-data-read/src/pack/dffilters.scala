package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object dffilters {


	def main(args:Array[String]):Unit={



			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly

			val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)   // RDD

			sc.setLogLevel("ERROR")

			val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

			import spark.implicits._
			val df = spark.read.format("csv").option("header","true")
			         .load("file:///D:/data/dtnew.txt")
			        
			df.show()         
			println
			println("==================")
			println
			println
			println("=======Category == Exercise===========")
			println			
			val filcat = df.filter(col("category")==="Exercise")
			filcat.show()
			println
			println("=======Category == Exercise and spendby =cash ===========")
			println						
			val filand = df.filter(
			                        col("category")==="Exercise"
			                        &&
			                        col("spendby")==="cash"
			        )
			filand.show()
			println
			println("=======Not(Category == Exercise and spendby =cash) ===========")
			println						
			val notfiland = df.filter(
			                        col("category")==="Exercise"
			                        &&
			                        col("spendby")==="cash"
			        )
			notfiland.show()
			println
			println("=======Category == Exercise or spendby =cash ===========")
			println						
			val filor = df.filter(
			                        col("category")==="Exercise"
			                        or
			                        col("spendby")==="cash"
			        )
			filor.show()
			println
			println("=======Not(Category == Exercise or spendby =cash) ===========")
			println						
			val notfilor = df.filter(!(
			                        col("category")==="Exercise"
			                        or
			                        col("spendby")==="cash"
			        ))
			notfilor.show()
			println
			println("=======Category == Exercise and Team Sports ===========")
			println					
			val filin = df.filter(col("category") isin ("Exercise","Team Sports"))
			filin.show()
			println
			println("=======NOT(Category == Exercise and Team Sports) ===========")
			println					
			val notfilin = df.filter(!(col("category") isin ("Exercise","Team Sports")))
			notfilin.show()
			println
			println("======== product like %Gymnastics%")
			println
			val fillike = df.filter(col("product") like "%Gymnastics%" )
			fillike.show()
			println
			println("======== product not like %Gymnastics%")
			println
			val notfillike = df.filter(!(col("product") like "%Gymnastics%" ))
			notfillike.show()
			println
			println("======== product is null======")
			println			
			val filnull = df.filter(col("product") isNull)
			filnull.show()
			println
			println("======== product is Not null======")
			println					
			val fillnotnull = df.filter(col("product") isNotNull)
			fillnotnull.show()
			
			
			println
			println("======== Testing shityy logic product is null======")
			println					
			val notfillnotnull = df.filter(!(col("product") isNotNull))
			notfillnotnull.show()
			
			println
			println("=======NOT (Category === Exercise)===========")
			println			
			val notfilcat = df.filter(!(col("category")==="Exercise"))
			notfilcat.show()
	}

}