/**
  * Created by mdhasanuzzamannoor on 12/25/16.
  */
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/Users/mdhasanuzzamannoor/Downloads/Softwares/spark-2.0.2-bin-hadoop2.7/READMEp.md" // Should be some file on your system

    //create a SparkContext object, which tells Spark how to access a cluster
    //The appName parameter is a name for your application to show on the cluster UI.
    // master is a Spark, Mesos or YARN cluster URL, or a special “local” string to run in local mode
    val conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application")

    //pass the SparkContext constructor a SparkConf object which contains information about our application.
    val sc = new SparkContext(conf)

    //make a new RDD from the text of the README file in the Spark source with two partitions
    val logData = sc.textFile(logFile, 2).cache()

    //Actions: Number of items(lines) in this RDD
    println("Number of items: ", logData.count())

    val lineLengths = logData.map(s => s.length)
    lineLengths.collect.foreach(println)

    val totalLength = lineLengths.reduce((a, b) => a + b)
    println("total Length", totalLength)

    val pairs = logData.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)
    counts.collect.foreach(println)

    //Actions: First item in this RDD
    println("First item: ", logData.first())

    //chaining transformations and actions
    val lineWithSpark = logData.filter(line=> line.contains("Spark")).count() // How many lines contain "Spark"?
    println("lines with spark: ", lineWithSpark)

    //This first maps a line to an integer value, creating a new RDD.
    // reduce is called on that RDD to find the largest line count.
    val lineWithMostWords = logData.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
    println("lines with most words: ", lineWithMostWords)

    //using scala/java library
    val lineWithMostWords2 = logData.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
    println("lines with most words: ", lineWithMostWords2)

    //combined the flatMap, map, and reduceByKey transformations
    // to compute the per-word counts in the file as an RDD of (String, Int) pairs.
    val wordCounts = logData.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCounts.take(5).foreach(println)

    //counts the number of lines containing ‘a’ and the number containing ‘b’
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    //create a parallelized collection holding the numbers 1 to 5
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    sc.stop()
  }
}
