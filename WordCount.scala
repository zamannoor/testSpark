import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.BigDecimal
/**
  * Created by mdhasanuzzamannoor on 12/22/16.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    //creating base RDD
    val wordslist = List("cat", "elephant", "rat", "rat", "cat")
    val wordsRDD = sc.parallelize(wordslist,4)

    //pluralize the words
    def makePlural(word:String):String = {
      word + "s"
    }
    println(makePlural("cat"))

    //pass each item in the base RDD into a map() transformation that applies the makePlural() function to each element
    //And then call the collect() action to see the transformed RDD.
    val pluralRDD = wordsRDD.map(makePlural)
    pluralRDD.collect().foreach(println)
    //or if you want to print a few lines : myRDD.take(n).foreach(println)

    //using anonymous function
    val pluralLambdaRDD = wordsRDD.map((word:String)=>word + "s")
    pluralLambdaRDD.collect().foreach(println)

    //length of each word
    val plurallengths = pluralRDD.map((word:String)=>word.length())
    plurallengths.collect().foreach(println)

    //create pairRDD
    val wordPairs = wordsRDD.map((word:String)=>(word,1))
    wordPairs.collect().foreach(println)

    //counting word pairs
    val wordsGrouped = wordPairs.reduceByKey(_+_)
    wordsGrouped.collect().foreach(println)

    //composing/chaining functions together
    val wordCountsCollected = (wordsRDD.map((word:String)=>(word,1)).reduceByKey(_+_))
    wordCountsCollected.collect().foreach(println)

    //find unique words
    val uniqueWords = wordCountsCollected.count()
    println(uniqueWords)

    //mean number of words per unique word in wordCounts
    val totalCount =  (wordsRDD.map((word:String)=>(1)).reduce(_+_))
    println(totalCount)
    val average = totalCount/uniqueWords.toDouble
    println(BigDecimal(average).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)

    //wordcount of a file
    def wordCount(wordListRDD: RDD[String]) :RDD[(String, Int)]={
      return wordListRDD.map((word:String)=>(word,1)).reduceByKey(_+_)
    }
    wordCount(wordsRDD).collect().foreach(println)

    def removePunctuation(text:String):String={
      val textLower = text.toLowerCase
      val textNoPunct = textLower.replaceAll("""[\p{Punct}]""", "")
      return textNoPunct.trim
    }

    println(removePunctuation("Hi, you!"))
    println(removePunctuation("No under_score!"))
    println(removePunctuation(" *      Remove punctuation then spaces  * "))

    val shakespeareRDD = sc.textFile("/Users/mdhasanuzzamannoor/workingdirectory/coursera Functional Programming Principles in Scala/Test3/src/test/scala-2.11/shakespeare.txt").map(removePunctuation)
    shakespeareRDD.zipWithIndex.map{ case (s,i) => (i,s) }.take(15).foreach(println)

    //split each line by its spaces
    val shakespeareWordsRDD =	shakespeareRDD.flatMap(line =>  line.split(" "))
    val shakespeareWordCount = shakespeareWordsRDD.count()
    shakespeareWordsRDD.take(5).foreach(println)
    println(shakespeareWordCount)


    //remove empty elements
    val shakeWordsRDD = shakespeareWordsRDD.filter(_.nonEmpty)
    val shakeWordCount = shakeWordsRDD.count()
    shakeWordsRDD.take(5).foreach(println)
    println(shakeWordCount)

    val top15WordsAndCounts = wordCount(shakeWordsRDD).takeOrdered(15)(Ordering[Int].reverse.on(x=>x._2))
    top15WordsAndCounts.zipWithIndex.map{ case (s,i) => (i,s) }.foreach(println)

  }
}
