
package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object dsl2 {


	def main(args:Array[String]):Unit={



			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly

			val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)   // RDD

			sc.setLogLevel("ERROR")

			val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

			import spark.implicits._
			val collist = List("","","","")


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
			                          "case when amount>200 then 'premium' else 'general' end as productcategory",
			                          "case when spendby='cash' then 1 else 0 end as status"
			                         	)

			exprdf.show()
			val withdf = df.withColumn( "category" ,  expr("upper(category)")  )
			               .withColumn("status" ,expr("case when spendby='cash' then 1 else 0 end"))
			               .withColumn("amount",expr("amount+1000"))
			               

			withdf.show()

	}

}
