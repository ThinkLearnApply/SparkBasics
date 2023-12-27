
package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source

object URLTask {
  
	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // put your path

			val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
			
			val sc = new SparkContext(conf)
			
			sc.setLogLevel("ERROR")
			
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			
			
			val urldata = Source.fromURL("https://randomuser.me/api/0.8/?results=3").mkString
			
			
			println(urldata)
			
			
			
			
			
			val df =  spark.read.json(sc.parallelize(List(urldata)))
			
			
			df.show()
			
			df.printSchema()
			
			
			val explodedf = df.withColumn("results",expr("explode(results)"))
			
			explodedf.show()
			
			explodedf.printSchema()
			
			val flattenedDf = explodedf.select(
			                  "nationality",
			                  "results.user.cell",
			                  "results.user.dob",
			                  "results.user.email",
			                  "results.user.gender",
			                  "results.user.location.city",
			                  "results.user.location.state",
			                  "results.user.location.street",
			                  "results.user.location.zip",
			                  "results.user.md5",
			                  "results.user.name.first",
			                  "results.user.name.last",
			                  "results.user.name.title",
			                  "results.user.password",
			                  "results.user.phone",
			                  "results.user.picture.large",
			                  "results.user.picture.medium",
			                  "results.user.picture.thumbnail",
			                  "results.user.registered",
			                  "results.user.salt",
			                  "results.user.sha1",
			                  "results.user.sha256",
			                  "results.user.username",
			                  "seed",
			                  "version"
			)
			
			flattenedDf.show()
			flattenedDf.printSchema()

	}

} 