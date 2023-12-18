package pack

import org.apache.spark._

object SparkDataRead {
  def main(args: Array[String]): Unit = {
    println("  == Spark Journey Started ==  ");
    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.hostname", "localhost")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///D:data/zf.txt") // change according your drive

    data.foreach(println)

    val statedata = sc.textFile("file:///D:/data/state.txt")
    println
    //expected output - To list : List(TN, UP) & List(Chennai, Prayagraj)
    println("====RAW DATA - RDD====")
    println
    statedata.foreach(println)
    println

    val split1 = statedata.flatMap(x => x.split("~"))
    println("====Split DATA - RDD====")
    println("====FLATENNED RDD====")
    println
    split1.foreach(println)
    println

    val stateList = split1.filter(x => x.contains("State"))
    println("====State Data - RDD====")
    println("====FILTERRED RDD====")
    println
    stateList.foreach(println)
    println

    val cityList = split1.filter(x => x.contains("City"))
    println("====City RDD====")
    println("====FILTERRED RDD====")
    println
    cityList.foreach(println)
    println

    val stateNameList = stateList.map(x => x.replace("State->", ""))
    println("====Final State Name RDD====")
    println("====MAPPED RDD====")
    println
    stateNameList.foreach(println)
    println

    val cityNameList = cityList.map(x => x.replace("City->", ""))
    println("====Final City Name RDD====")
    println("====MAPPED RDD====")
    println
    cityNameList.foreach(println)
    println

  }

}