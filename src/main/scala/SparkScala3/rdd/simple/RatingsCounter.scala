package SparkScala3.rdd.simple

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("./src/main/scala/data/ml-100k/u.data")

    val raw = lines.map(x => (x.toString.split("\t")(2).toInt,1))
    val summation = raw.reduce((x,y) => (x._1.toInt + y._1.toInt,x._2.toInt + y._2.toInt))
    val avg = (summation._1)/(summation._2);
    println(f"summation: ${summation._1} count: ${summation._2} average: ${avg}")


    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(2))



    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}
