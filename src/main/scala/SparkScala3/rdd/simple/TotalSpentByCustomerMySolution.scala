package SparkScala3.rdd.simple

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalSpentByCustomerMySolution {

  def extractCustomerIdPricePairs(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt,fields(2).toFloat)
  }


  def main(args: Array[String])  {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","TotalSpentByCustomerMySolution")

    val input = sc.textFile("./src/main/scala/data/customer-orders.csv")

    val mappedInput = input.map(extractCustomerIdPricePairs)

    val totalSpendsPerCustomer = mappedInput.reduceByKey((x,y)=>x+y)

    val flipped = totalSpendsPerCustomer.map(x=>(x._2,x._1))

    val totalSpendsPerCustomerSorted = flipped.sortByKey()

    totalSpendsPerCustomerSorted.foreach(println)

    println()

    val totalByCustomer = mappedInput.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))

    val averageSpendsById = totalByCustomer.mapValues(tuple => tuple._1 /  tuple._2)

    val flippedAverages = averageSpendsById.map(x=>(x._2,x._1))

    val averageSpendsByIdSorted = flippedAverages.sortByKey()

    averageSpendsByIdSorted.foreach(println)

  }
}
