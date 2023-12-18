

package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object dslDay2 {


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
			println("========category = Exercise==========")
			println


			val exprdf = df.selectExpr(
			    
			                          "id",
			                          "tdate",
			                          "amount",
			                          "upper(category) as category",
			                          "product",
			                          "spendby",
			                          "case when amount > 200 then 1 else 0 end as greaterthan200",
			                          "case when spendby='cash' then 0 else 1 end as status"
			)

			exprdf.show()

	}

}
