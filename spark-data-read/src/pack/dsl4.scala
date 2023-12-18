
package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object dsl4 {


	def main(args:Array[String]):Unit={



			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // Put your drive accordingly

			val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)   // RDD

			sc.setLogLevel("ERROR")

			val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

			import spark.implicits._
			val collist = List("","","","")


			val proddf = spark.read.format("csv").option("header","true")
			.load("file:///D:/data/prodn.csv")

			proddf.show()         

			val custdf = spark.read.format("csv").option("header","true")
			.load("file:///D:/data/custn.csv")

			custdf.show()         
			
			
			println
			println("========fulljoin==========")
			println

			val fulljoin = custdf.join(proddf,Seq("id"), "Full").orderBy("id")
			fulljoin.show();

			val leftjoin = custdf.join(proddf,Seq("id"), "left").orderBy("id")
			leftjoin.show();
			
			val rightjoin = custdf.join(proddf,Seq("id"), "right").orderBy("id")
			rightjoin.show();
			
			val innerjoin = custdf.join(proddf,Seq("id"), "inner").orderBy("id")
			innerjoin.show();
						
			val leftantijoin = custdf.join(proddf,Seq("id"), "left_anti").orderBy("id")
			leftantijoin.show();
			
			val rightantijoin = proddf.join(custdf,Seq("id"), "left_anti").orderBy("id")
			rightantijoin.show();
	}

}
