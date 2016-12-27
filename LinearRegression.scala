import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by mdhasanuzzamannoor on 12/22/16.
  */
object LinearRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Linear Regression")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder
      .config(conf = conf)
      .appName("Linear Regression")
      .getOrCreate()


    val path = "src/test/scala-2.11/millionsong.txt"
    val raw_data_df =  spark.read.text(path)
    raw_data_df.take(3).foreach(println)

    val raw_data_rdd = sc.textFile(path)
    raw_data_rdd.take(3).foreach(println)

    val num_points = raw_data_df.count()
    println(num_points)

    raw_data_df.show()
    raw_data_df.printSchema()
    raw_data_df.select("value").show()


    val schemaString = "label features"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = raw_data_rdd
      .map(_.split(","))
      .map(attributes => Row(attributes(0), for (i <- 1 until attributes.length) yield attributes(i)))

    rowRDD.take(5).foreach(println)
    rowRDD.map(t => "Label: " + t(0)).take(5).foreach(println)
    println(rowRDD.first().get(0))

    //val lptDf = rowRDD.map(rowLst => LabeledPoint(rowRDD.first().getAs[Double](0), rowRDD.first().getAs[Vector](for(i <- 1 until rowRDD.count()) yield i))).toDF()

    //val dataPoints = rowRDD.map(row =>
      //new LabeledPoint(
        //row.last.toDouble,
        //Vectors.dense(row.take(row.length - 1).map(str => str.toDouble))
      //)
    //)

    //val obsDF = raw_data_df.select(raw_data_df.map(_.split(raw_data_df.select("value"),",")))
    //def parse_points(df: DataFrame): DataFrame= {
      //val obsDf = df.select(sql.functions.split(df("value"),","))
      //val lptDf = obsDf.rdd.map(rowLst: String=> LabeledPoint(float(rowLst(0)(0)), (float(x) for x in rowLst(0)(1:)))).toDF()
      //return lptDf

    //}

    //val parsed_points_df = parse_points(raw_data_df)
   // println(parsed_points_df.count())



  }
}
