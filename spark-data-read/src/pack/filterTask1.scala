package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object filterTask1 {
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
			println("========upper(category) - dataframe.selectExp(comma separated column) ==========")
			println
			val edf = df.selectExpr(
			                    "id",
			                    "tdate",
			                    "amount",
			                    "upper(category) as category",
			                    "product",
			                    "spendby"
			
			                )
			edf.show()	
			
			println
			println("=========spendby not equals to cash=========")
			println

			val nocashfil = df.filter(!(col("spendby")==="cash"))
			nocashfil.show()
			nocashfil.createOrReplaceTempView("nocash")
			println
			println("=========data with additional column status having value zeyo=========")
			println
			spark .sql("select *, 'zeyo' as status from nocash").show()
			
      println
			println("=========uber data with additional column day =========")
			println
			val uberdf = spark.read.format("csv").option("header", true).load("file:///D:/data/uber")
			uberdf.createOrReplaceTempView("uber")
			val uberdfwithday = spark.sql("select *, date_format(TO_DATE(date,'mm/dd/yyyy'),'EEEE') as day from uber")
      uberdfwithday.show()
			println(uberdf.count())

			
			
			
	}
}