package pack


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object readDifferentFileFormat {
  case class schema(id:Int,category:String,product:String)

def main(args:Array[String]):Unit={
    System.setProperty("hadoop.home.dir","D:\\hadoop")
		println("===started===")
		println

		val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

		val sc = new SparkContext(conf)   // RDD

		sc.setLogLevel("ERROR")

		val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

		import spark.implicits._


		val parquetdf = spark.read.format("parquet").load("file:///D:/data/parfile.parquet")
		println("==================== PARQUET Data Frame ==================== ")
		parquetdf.show()
		
		println
		println
		println
		val orcdf = spark.read.format("orc").load("file:///D:/data/orcfile.orc")
		println("==================== ORC Data Frame ==================== ")
		orcdf.show()
		println
		println
		println("orcdf.createOrReplaceTempView(\"orcdata\")")
		orcdf.createOrReplaceTempView("orcdata")
		
		println("==================== select  * from orcdata where age > 10 ==================== ")
		val procorcdf = spark.sql("select  * from orcdata where age > 10")
		procorcdf.show(100)
		println
		println
		println
		
		
		
		
		println("==================== AVRO Data Frame ==================== ")
		val avrodf = spark.read.format("avro").load("file:///D:/data/part.avro")
		avrodf.show()
		
		val jsondf = spark.read.format("json").load("file:///D:/data/devices.json")
		println("==================== JSON Data Frame ==================== ")
		jsondf.show()
		jsondf.createOrReplaceTempView("jdf")
		
		println("==================== select  * from jdf where device_id < 10 ==================== ")
		val procdf = spark.sql("select  * from jdf where device_id < 10").dropDuplicates("device_id").orderBy("device_id")
		procdf.show()
		
		
		val procdf1 = spark.sql("select * from jdf where humidity > 50")
		procdf1.show()
		println("==================== Writing Data to CSV File ==================== ")
		procdf1.write.format("csv").option("header","true").mode("overwrite").save("file:///D:/data/newcsvwrite")  // Put your path to write
		println("==================== Writing Data to CSV File Completed ==================== ")	
		
		println("==================== Writing Data to Parquet File ==================== ")
		procdf1.write.format("parquet").mode("overwrite").save("file:///D:/data/parquet")  // Put your path to write
		println("==================== Writing Data to Parquet File Completed ==================== ")	
		
		
		
		

}
  
}