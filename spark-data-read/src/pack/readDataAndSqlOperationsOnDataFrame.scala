
 
  package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object readData {


case class schema(id : Double, category :String ,product :String)


def main(args:Array[String]):Unit={

		println("===started===")
		println

		val conf = new SparkConf().setAppName("wcfinal").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")

		val sc = new SparkContext(conf)   // RDD

		sc.setLogLevel("ERROR")
    println
    println("================ Creating spark session ================")
		println
		println("val conf = new SparkConf().setAppName(\"wcfinal\").setMaster(\"local[*]\").set(\"spark.driver.host\",\"localhost\").set(\"spark.driver.allowMultipleContexts\", \"true\")")
		println("val spark  = SparkSession.builder().config(conf).getOrCreate()")
		val spark  = SparkSession.builder().config(conf).getOrCreate() //Dataframe

		import spark.implicits._

		println
		println("================ To read any format of data we should remember the below line  ================")
		println
		println("================ spark  .  read  .  format  .  option  .  load ================")
		println
		println("================ Parquet, JSON, JDBC, ORC, NOSQL, CASSANDRA, MONGODB, CSV etc and many more format can be read  ================")
		println
		println("val df = spark.read.format(\"csv\").option(\"header\",\"true\").load(\"file:///D:/data/df.csv\")")
		val df = spark.read.format("csv").option("header","true").load("file:///D:/data/df.csv")
		println("df.show()")
		df.show()
		
		println("val df1 = spark.read.format(\"csv\").option(\"header\",\"true\").load(\"file:///D:/data/df1.csv\")")
		val df1 = spark.read.format("csv").option("header","true").load("file:///D:/data/df1.csv")
		println("df1.show()")
		df1.show()		

		println("val prod = spark.read.format(\"csv\").option(\"header\",\"true\").load(\"file:///D:/data/prod.csv\")")
		val prod = spark.read.format("csv").option("header","true").load("file:///D:/data/prod.csv")
		println("prod.show()")
		prod.show()		

		println("val cust  = spark.read.format(\"csv\").option(\"header\",\"true\").load(\"file:///D:/data/cust.csv\")")
		val cust = spark.read.format("csv").option("header","true").load("file:///D:/data/cust.csv")
		println("cust.show()")
		cust.show()	
		
		println
		println("================ Giving name to data frame ================")
		println("df.createOrReplaceTempView(\"df\")")
		df.createOrReplaceTempView("df")
		println
		println("df1.createOrReplaceTempView(\"df1\")")
		df1.createOrReplaceTempView("df1")
		println
		println("prod.createOrReplaceTempView(\"prod\")")
		prod.createOrReplaceTempView("prod")
		
		println
		println("cust.createOrReplaceTempView(\"cust\")")
		cust.createOrReplaceTempView("cust")
		
		sc.setLogLevel("ERROR")
		
		println
		println("================To do any operation using sql on Data Frame we must use the below format ================")
		println("spark.sql(\"SQL Statement\")")
		
		println
		println("================Order By ================")
		println("select * from df order by id")
		println
		spark.sql("select * from df order by id").show()
		
		println
		println("================Order By ================")
		println("select * from df1 order by id")
		println
		spark.sql("select * from df1 order by id").show()
		
		println
		println("================Validate Data================")
		println("select * from df")
		println
		spark.sql("select * from df").show()
		
		println
		println("================Select Two Columns================")
		println("select id, tDate from df order by id")
		println
		spark.sql("select id, tDate from df order by id").show()

		println
		println("================Select Columns With category=\"Exercise\" ================")
		println("select id, tDate, category from df where category='Exercise' order by id")
		println
		spark.sql("select id, tDate, category from df where category='Exercise' order by id").show()
		
		println
		println("================Multi Column Filter ================")
    println("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash' ")
    println
		spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash' ").show()
		
		println
		println("================Multi Value Filter ================")
		println("select * from df where category in ('Exercise','Gymnastics')")
		println
		spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()
		
		println
		println("================Like Filter ================")
		println("select * from df where product like ('%Gymnastics%')")
		println
		spark.sql("select * from df where product like ('%Gymnastics%')").show()
		
		println
		println("================Not Filter ================")
		println("select * from df where category != 'Excercise'")
		println
		spark.sql("select * from df where category != 'Excercise'").show()
		
		println
		println("================Not In Filter ================")
		println("select * from df where category  not in ('Excercise','Gymnastics')")
		println
		spark.sql("select * from df where category  not in ('Excercise','Gymnastics')").show()

		println
		println("================Null Filter ================")
		println("select * from df where product is null")
		println
		spark.sql("select * from df where product is null").show()
		
		println
		println("================Not Null Filter ================")
		println("select * from df where product is not null")
		println
		spark.sql("select * from df where product is not null").show()
		
		println
	  println("================Max Function ================")
		println("select max(id) from df")
		println
		spark.sql("select max(id) from df").show()
    
		println
		println("================Min Function ================")
		println("select min(id) from df")
		println
		spark.sql("select min(id) from df").show()
		
		println
		println("================Count ================")
		println("select count(1) from df")
		println
		spark.sql("select count(1) from df").show()
		
		println
		println("================Condition Statement ================")
		println("select *, case when spendby='cash' then 1 else 0 end as status from df")
		println
		spark.sql("select *, case when spendby='cash' then 1 else 0 end as status from df").show()
		
		println
		println("================Concat Data ================")
		println("select id,category,concat(id,'-',category) as condata from df")
		println
		spark.sql("select id,category,concat(id,'-',category) as condata from df").show()
		
		
		println
		println("================Concat_ws data ================")
		println("select concat_ws('-',id,category,product) as concat from df ")
		println
		spark.sql("select concat_ws('-',id,category,product) as concat from df ").show()
		
		println
		println("================Lower Case data ================")
		println("select category,lower(category) as lower from df ")
		println
		spark.sql("select category,lower(category) as lower from df ").show()

		println
		println("================Ceil data ================")
		println("select amount,ceil(amount) from df")
		println
		spark.sql("select amount,ceil(amount) from df").show()
  
		println
		println("================Round data ================")
		println("select amount,round(amount) from df")
		println
		spark.sql("select amount,round(amount) from df").show()

		println
		println("================Replace Nulls ================")
		println("select coalesce(product,'NA') from df")
		println
		spark.sql("select coalesce(product,'NA') from df").show()

		println
		println("================Trim the space ================")
		println("select trim(product) from df")
		println
		spark.sql("select trim(product) from df").show()

		println
		println("================Distinct the columns ================")
		println("select distinct category,spendby from df")
		println
		spark.sql("select distinct category,spendby from df").show()

		println
		println("================Substring with Trim ================")
		println("select substring(trim(product),1,10) from df")
		println
		spark.sql("select substring(trim(product),1,10) from df").show()

		println
		println("================Substring/Split operation ================")
		println("select SUBSTRING_INDEX(category,' ',1) as spl from df")
		println
		spark.sql("select SUBSTRING_INDEX(category,' ',1) as spl from df").show()

		println
		println("================Union All ================")
		println("select * from df union all select * from df1")
		println
		spark.sql("select * from df union all select * from df1").show()

		println
		println("================Union ================")
		println("select * from df union select * from df1 order by id")
		println
		spark.sql("select * from df union select * from df1 order by id").show()

		println
		println("================Aggregate Sum ================")
		println("select category, sum(amount) as total from df group by category")
		println
		spark.sql("select category, sum(amount) as total from df group by category").show()

		println
		println("================Aggregate Sum With Two Columns ================")
		println("select category,spendby,sum(amount) as total from df group by category,spendby")
		println
		spark.sql("select category,spendby,sum(amount) as total from df group by category,spendby").show()

		println
		println("================Aggregate Count ================")
		println("select category,spendby,sum(amount) As total,count(amount) as cnt from df group by category,spendby")
		println
		spark.sql("select category,spendby,sum(amount) As total,count(amount) as cnt from df group by category,spendby").show()

		println
		println("================Aggregate Max ================")
		println("select category,count(category) as cnt from df group by category having count(category)>1")
		println
		spark.sql("select category, max(amount) as max from df group by category").show()

		println
		println("================Aggregate Max Order Ascending ================")
		println("select category, max(amount) as max from df group by category order by category")
		println
		spark.sql("select category, max(amount) as max from df group by category order by category").show()

		println
		println("================Aggregate with Order Descending ================")
		println("select category, max(amount) as max from df group by category order by category desc")
		println
		spark.sql("select category, max(amount) as max from df group by category order by category desc").show()

		println
		println("================Window Row Number ================")
		println("SELECT category,amount, row_number() OVER ( partition by category order by amount desc ) AS row_number FROM df")
		println
		spark.sql("SELECT category,amount, row_number() OVER ( partition by category order by amount desc ) AS row_number FROM df").show()

		println
		println("================Window dense_rank Function ================")
		println("SELECT category,amount, dense_rank() OVER ( partition by category order by amount desc ) AS dense_rank FROM df")
		println
		spark.sql("SELECT category,amount, dense_rank() OVER ( partition by category order by amount desc ) AS dense_rank FROM df").show()

		println
		println("================Window Rank Function ================")
		println("SELECT category,amount, rank() OVER ( partition by category order by amount desc ) AS rank FROM df")
		println
		spark.sql("SELECT category,amount, rank() OVER ( partition by category order by amount desc ) AS rank FROM df").show()

		println
		println("================Window lead Function ================")
		println("SELECT category,amount, lead(amount) OVER ( partition by category order by amount desc ) AS lead FROM df")
		println
		spark.sql("SELECT category,amount, lead(amount) OVER ( partition by category order by amount desc ) AS lead FROM df").show()

		println
		println("================Window lag function ================")
		println("SELECT category,amount, lag(amount) OVER ( partition by category order by amount desc ) AS lag FROM df")
		println 
		spark.sql("SELECT category,amount, lag(amount) OVER ( partition by category order by amount desc ) AS lag FROM df").show()

		println
		println("================Group By Having Function ================")
		println("select category,count(category) as cnt from df group by category having count(category)>1")
		println
		spark.sql("select category,count(category) as cnt from df group by category having count(category)>1").show()

		println
		println("================iNNER Join ================")
		println("select a.id,a.name,b.product from cust a join prod b on a.id=b.id")
		println
		spark.sql("select a.id,a.name,b.product from cust a join prod b on a.id=b.id").show()

		println
		println("================Left Join ================")
		println("select a.id,a.name,b.product from cust a left join prod b on a.id=b.id")
		println
		spark.sql("select a.id,a.name,b.product from cust a left join prod b on a.id=b.id").show()

		println
		println("================Right Join ================")
		println("select a.id,a.name,b.product from cust a right join prod b on a.id=b.id")
		println
		spark.sql("select a.id,a.name,b.product from cust a right join prod b on a.id=b.id").show()

		println
		println("================Full Join ================")
		println("select a.id,a.name,b.product from cust a full join prod b on a.id=b.id")
		println
		spark.sql("select a.id,a.name,b.product from cust a full join prod b on a.id=b.id").show()

		println
		println("================Left Anti Join ================")
		println("select a.id,a.name from cust a LEFT ANTI JOIN prod b on a.id=b.id")
		println
		spark.sql("select a.id,a.name from cust a LEFT ANTI JOIN prod b on a.id=b.id").show()

		println
		println("================Left Semi Join ================")
		println("select a.id,a.name from cust a LEFT SEMI JOIN prod b on a.id=b.id")
		println
		spark.sql("select a.id,a.name from cust a LEFT SEMI JOIN prod b on a.id=b.id").show()

		println
		println("================Date Format ================")
		println("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df")
		println
		spark.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df").show()

		println
		println("================Sub query ================")
		println("""
      select sum(amount) as total , con_date from(
      select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date,amount,category,product,spendby from df)
      group by con_date
    """)
		println
		spark.sql("""
      select sum(amount) as total , con_date from(
      select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date,amount,category,product,spendby from df)
      group by con_date
    """).show()
  
  }

}