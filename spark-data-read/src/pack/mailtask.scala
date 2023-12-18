package pack


import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object mailtask {

	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // put your path


			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._

			val df = spark.read.format("csv")
			.option("header",true)
			.load("file:///D:/data/mail.csv")

			df.show()   


			val iddf = df.withColumn("idname",expr("split(Mail,'@')[0]"))
			.withColumn("com",expr("split(Mail,'@')[1]")) 


			iddf.show()

			val firstlast = iddf.withColumn("first",expr("substring(idname,0,1)"))
			.withColumn("last",expr("substring(idname,-1,1)")) 
			.withColumn("firstnum",expr("substring(mob,0,2)"))
			.withColumn("lastnum",expr("substring(mob,-2,2)")) 


			firstlast.show()		


			val size = firstlast.withColumn( "size" , length(col("idname"))-2 ) 
			
			.withColumn( "mobsize" , length(col("mob"))-4 ) 
			
			
			.withColumn("mailstar",
					expr("concat(first,repeat('*',size),last)")
					)
			.withColumn("finalmobile",
					expr("concat(firstnum,repeat('*',mobsize),lastnum)")
					)

			size.show()


			val concatemail = size.withColumn("FinalMail",expr("concat(mailstar,'@',com)"))
			concatemail.show()
			
			val finaldf = concatemail.select("FinalMail","finalmobile")

			finaldf.show()



	}

}