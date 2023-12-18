package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object obj {

  
  case class column(id:String,category:String,product:String)
	def main(args:Array[String]):Unit={



			println("===started===")
			println

			val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]")
			val sc = new SparkContext(conf)   // RDD
			sc.setLogLevel("ERROR")
			val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe
			val st_datatxns = sc.textFile("file:///D:/data/datatxns")
      println("==========DATATXNS============")
      println("==========TASK 1============")
      println
      println
			st_datatxns.foreach(println)
			println
			
			val split = st_datatxns.map(x=>x.split(","))
			println
			
			val schemardd = split.map(x => column(x(0),x(1),x(2)) )
			
			val filterRdd = schemardd.filter(x => x.product.contains("Gymnastic"))
			
      println
      println
      filterRdd.foreach(println)
      
      println
      println
      println("==========Next USDATA============")
			println
			println
			val st_usdata = sc.textFile("file:///D:/data/usdata",1)
			st_usdata.foreach(println)
      println


			st_usdata.take(10).foreach(println)
			println
      
			val recordLengthMoreThan200 = st_usdata.filter(x=>x.length()>200)
			recordLengthMoreThan200.foreach(println)
			println
      
			
			val flatData = recordLengthMoreThan200.flatMap(x=>x.split(","))
			flatData.foreach(println)
			println
			
			val repData = flatData.map(x=>x.replace("-",""))
			repData.foreach(println)
			println
			
			val conData = flatData.map(x=>x.concat("zeyo"))
			conData.foreach(println)
			println
			
		//	conData.saveAsTextFile("file:///D:/data/out")
		//	println("========= Data Written ==========")
		//	val splitUsingFlatMap = st_usdata.map(x=>x.split(",")(3))
		//	splitUsingFlatMap.foreach(println)
		//	println
			println
			println
			println
			println
      println("==========TASK 2============")
			println
			println
			println
			val filter_usdata = st_usdata.filter(x => x.contains("LA"))
			val flattened_usdata = filter_usdata.flatMap(x => x.split(","))
			flattened_usdata.foreach(println)
			
	}

}