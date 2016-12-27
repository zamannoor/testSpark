import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

/**
  * Created by mdhasanuzzamannoor on 12/27/16.
  */
object LinearRegressionExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Linear Regression Example")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Linear Regression example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Load training data
    val path = "src/test/scala-2.11/sample_linear_regression_data.txt"
    val training = spark.read.format("libsvm")
      .load(path)

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

  }
}
