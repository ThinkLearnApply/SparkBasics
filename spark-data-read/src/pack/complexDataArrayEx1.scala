package pack

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object complexDataArrayEx1 {
  
	def main(args:Array[String]):Unit={

			System.setProperty("hadoop.home.dir", "D:\\hadoop")   // put your path

			val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			val actorjsondf = spark
			              .read
			              .format("json")
			              .option("multiline","true")
			              .load("file:///D:/data/actorsj.json")
			              
			actorjsondf.show()
			actorjsondf.printSchema()
			
			/*root
 |-- Actors: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Birthdate: string (nullable = true)
 |    |    |-- BornAt: string (nullable = true)
 |    |    |-- age: long (nullable = true)
 |    |    |-- hasChildren: boolean (nullable = true)
 |    |    |-- hasGreyHair: boolean (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- photo: string (nullable = true)
 |    |    |-- picture: struct (nullable = true)
 |    |    |    |-- large: string (nullable = true)
 |    |    |    |-- medium: string (nullable = true)
 |    |    |    |-- thumbnail: string (nullable = true)
 |    |    |-- weight: double (nullable = true)
 |    |    |-- wife: string (nullable = true)
 |-- country: string (nullable = true)
 |-- version: string (nullable = true)
			 * */
			
			println("***** In order to flatened the Json array we can use both explode(elementName) and select as well")
			
			val explodeddf = actorjsondf.withColumn("Actors",expr("explode(Actors)"))
			explodeddf.show()
			explodeddf.printSchema()
			
			/*
			 * root
 |-- Actors: struct (nullable = true)
 |    |-- Birthdate: string (nullable = true)
 |    |-- BornAt: string (nullable = true)
 |    |-- age: long (nullable = true)
 |    |-- hasChildren: boolean (nullable = true)
 |    |-- hasGreyHair: boolean (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- photo: string (nullable = true)
 |    |-- picture: struct (nullable = true)
 |    |    |-- large: string (nullable = true)
 |    |    |-- medium: string (nullable = true)
 |    |    |-- thumbnail: string (nullable = true)
 |    |-- weight: double (nullable = true)
 |    |-- wife: string (nullable = true)
 |-- country: string (nullable = true)
 |-- version: string (nullable = true)
			 * 
			 * */
			
			val flateneddf = explodeddf.select(
			    
			                              "Actors.Birthdate",
			                              "Actors.BornAt",
			                              "Actors.age",
			                              "Actors.hasChildren",
			                              "Actors.hasGreyHair",
			                              "Actors.name",
			                              "Actors.photo",
			                              "Actors.picture.large",
			                              "Actors.picture.medium",
			                              "Actors.picture.thumbnail",
			                              "Actors.weight",
			                              "Actors.wife",
			                              "country",
			                              "version"
			)
			flateneddf.show()
			flateneddf.printSchema()
			
			/*
			 * root
 |-- Birthdate: string (nullable = true)
 |-- BornAt: string (nullable = true)
 |-- age: long (nullable = true)
 |-- hasChildren: boolean (nullable = true)
 |-- hasGreyHair: boolean (nullable = true)
 |-- name: string (nullable = true)
 |-- photo: string (nullable = true)
 |-- large: string (nullable = true)
 |-- medium: string (nullable = true)
 |-- thumbnail: string (nullable = true)
 |-- weight: double (nullable = true)
 |-- wife: string (nullable = true)
 |-- country: string (nullable = true)
 |-- version: string (nullable = true)
			 * */
			
			val newExplodedJson = explodeddf.withColumn("Actors",expr("explode(Actors)"))
			explodeddf.show()
			explodeddf.printSchema()
			

	}

}