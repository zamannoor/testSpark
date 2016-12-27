import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg._
import breeze.numerics._

/**
  * Created by mdhasanuzzamannoor on 12/22/16.
  */
object Test3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Test3")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session")
      .getOrCreate()
    val pathEmployee = "/Users/mdhasanuzzamannoor/workingdirectory/coursera Functional Programming Principles in Scala/Test3/src/test/scala-2.11/employee.json"
    val dfEmpl = sparkSession.read.option("header","true").json(pathEmployee)
    //see all data of the dataframe in a tabular format
    dfEmpl.show()
    //schema of the dataframe
    dfEmpl.printSchema()
    //fetch a specific column from all the columns
    dfEmpl.select("name").show()
    //filter by a condition
    dfEmpl.filter(dfEmpl("age") > 23).show()
    //grouping by a column
    dfEmpl.groupBy("age").count().show()

    val pathShakespeare = "/Users/mdhasanuzzamannoor/workingdirectory/coursera Functional Programming Principles in Scala/Test3/src/test/scala-2.11/shakespeare.txt"
    val dfShkpr = sparkSession.read.option("header","false").text(pathShakespeare)
    //count all the words in the document
    val shakespeareCount = dfShkpr.count()
    println(shakespeareCount)

    val x = DenseVector(1.0, 2.0, 3.0)
    //scalar multiplication
    val r = x * 2.0
    println(r)

    val y = DenseVector(4.0, 5.0, 6.0)
    //element-wise multiplication
    val r2 = x:*y
    println(r2)

    //dot product
    val r3 = x dot y
    println(r3)

    //matrix multiplication
    val m = DenseMatrix((1,2),(3,4))
    val n = DenseMatrix((5),(6))
    val r4 = m*n
    println(r4)

    val transpose = m.t
    println(transpose)
    //multiply m times its transpose ( A^T ) and then calculate the inverse of the result (AA^t)^-1
    val MMt = m*transpose
    val MMtinv = inv(MMt)
    val roundMMtinv = round(MMtinv)
    println(MMtinv)
    println(roundMMtinv)


    //slicing to get last two elements
    val lastTwo = x(-2 to -1)
    println(lastTwo)

    //combining objects
    val zeros = DenseVector.zeros[Double](8)
    val ones = DenseVector.ones[Double](8)
    val zerosBeforeOnes = DenseVector.horzcat(zeros, ones)
    val zerosThenOnes = DenseVector.vertcat(zeros, ones)
    println(zerosBeforeOnes)
    println(zerosThenOnes)

    //Anonymous function
    val addSLambda =  ((xx:String) => xx + "s")
    println(addSLambda)
    println(addSLambda("cat"))

    val multiplyByTen = ((yy:Int) => yy*10)
    println(multiplyByTen(5))


    sc.stop()
  }
}