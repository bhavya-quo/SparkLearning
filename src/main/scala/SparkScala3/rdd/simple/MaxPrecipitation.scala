package SparkScala3.rdd.simple

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.math.max

/** Find the maximum temperature by weather station for a year */
object MaxPrecipitation {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val precipitation = fields(3)
    (stationID, entryType, precipitation)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxPrecipitation")

    val lines = sc.textFile("./src/main/scala/data/1800.csv")
    val parsedLines = lines.map(parseLine)
    val maxPrecps = parsedLines.filter(x => x._2 == "PRCP")
    val stationPrecps = maxPrecps.map(x => (x._1, x._3.toInt))
    val maxPrecpByStation = stationPrecps.reduceByKey( (x,y) => max(x,y))
    val results = maxPrecpByStation.collect()

    for (result <- results.sorted) {
       val station = result._1
       val precp = result._2
       println(s"$station max precipitation: $precp")
    }

  }
}
