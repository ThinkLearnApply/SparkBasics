package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkrevision {
  
  
  case class schema
              (
                  txnno:String,
                  txndate:String,
                  custno:String,
                  amount:String,
                  category:String,
                  product:String,
                  city:String,
                  state:String,
                  spendby:String
                  )
  
  
  

	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // put your path

			val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
			
			val sc = new SparkContext(conf)
			
			sc.setLogLevel("ERROR")
			
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			
				val collist = List("txnno","txndate","custno","amount","category","product","city","state","spendby")
		
			
			val file1 = sc.textFile("file:///D:/data/file1.txt")
			
			file1.take(5).foreach(println)
			
			
			println
			println("========gymrows========")
			println
			
			
			val gymrow = file1.filter( x => x.contains("Gymnastics"))
			
			
			gymrow.take(5).foreach(println)
			
			
			
			
			val mapsplit = gymrow.map( x  =>  x.split(","))
			
			val schemardd = mapsplit.map( x => schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
			
			val fildata = schemardd.filter(x => x.product.contains("Gymnastics"))
			
			
			println
			println("========prod gymnastics========")
			println			
			
			fildata.take(5).foreach(println)
			
			println
			println("========schemadf========")
			println		      
      
			
			
			val schemadf = fildata.toDF().select(collist.map(col):_*)
			
			schemadf.show(5)
			
			
 			println
			println("========csvdf========")
			println		      
			
			
			val csvdf = spark.read.format("csv").option("header","true")
			            .load("file:///D:/data/file3.txt").select(collist.map(col):_*)
			
			csvdf.show(5)
			
 			println
			println("========jsondf========")
			println					
			
			
			val jsondf = spark.read.format("json")
			              .load("file:///D:/data/file4.json").select(collist.map(col):_*)
			
			jsondf.show(5)
			
 			println
			println("========Parquet data frame========")
			println					
			
			val parquetdf = spark.read
			                .load("file:///D:/data/file5.parquet").select(collist.map(col):_*)
			
			
			parquetdf.show(5)
			
			
 			println
			println("========xmldf========")
			println				
			
			
			val xmldf = spark.read.format("xml").option("rowtag","txndata")
			            .load("file:///D:/data/file6").select(collist.map(col):_*)
			
			xmldf.show(5)
			
			
	
			
			
			val uniondf = schemadf.union(csvdf).union(jsondf).union(parquetdf).union(xmldf)
			
			
			println
			println("=======union data frame=======")
			println
			
			uniondf.show(5)
			
			
			println
			println("=======Proc data frame=======")
			println			
			
			
			
			
			val procdf = uniondf.withColumn("txndate",expr("split(txndate,'-')[2]"))
			                    .withColumnRenamed("txndate","year")
			                    .withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
			                    .filter(col("txnno") > 50000)
			
			procdf.show(5)
			
			
			
			
			println
			println("=======aggregated amount data frame=======")
			println					
			
			
			val aggdf = procdf.groupBy("category")
			                  .agg(sum("amount").as("total"))
			
			aggdf.show()
			
			println
			println("=======coalesce is to reduce partition(everything to be written in single file)=======")
			println					
			
			
			
			aggdf.coalesce(1).write.format("csv").mode("overwrite").save("file:///D:/data/revdata")
			
			
			
			
			
			

	}

}