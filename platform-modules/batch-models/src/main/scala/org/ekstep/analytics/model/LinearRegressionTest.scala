package org.ekstep.analytics.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{ RandomForestRegressionModel, RandomForestRegressor }
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{ Vector, Vectors }
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.apache.spark.ml.regression.LinearRegression

object LinearRegressionTest {

    def main(args: Array[String]): Unit = {
        val sc = CommonUtil.getSparkContext(2, "test")
        implicit val sqlContext = new SQLContext(sc);
        import sqlContext.implicits._

        val inputFile = if(args.length != 0 && null != args(0)) args(0) else "data/input/train.dat.libfm"
        // Load and parse the data file, converting it to a DataFrame.
        val input = sc.textFile(inputFile)
        val vecSize = input.map(_.trim)
            .filter(line => !(line.isEmpty || line.startsWith("#")))
            .map { line =>
                val items = line.split(' ')
                val indices = items.tail.filter(_.nonEmpty).map { item =>
                    val indexAndValue = item.split(':')
                    indexAndValue(0).toInt
                }
                indices
            }.flatMap { x => x }.max()

        val x = input.map(_.trim)
            .filter(line => !(line.isEmpty || line.startsWith("#")))
            .map { line =>
                val items = line.split(' ')
                val label = items.head.toDouble
                val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
                    val indexAndValue = item.split(':')
                    val index = indexAndValue(0).toInt // Convert 1-based indices to 0-based.
                    val value = indexAndValue(1).toDouble
                    (index, value)
                }.unzip
                LabeledPoint(label, Vectors.sparse(vecSize + 1, indices.toArray, values.toArray))
            }
        val data = x.toDF()

        val maxIter = if(args.length > 2 && null != args(2)) args(2).toInt else 1000
        val regParam = if(args.length > 3 && null != args(3)) args(3).toDouble else 0.0
        val elasticNetParam = if(args.length > 4 && null != args(4)) args(4).toDouble else 0.00001
        val outputFile = if(args.length > 1 && null != args(1)) args(1) else "data/output/LR-predictions.txt"
        
        val lr = new LinearRegression()
            .setMaxIter(maxIter)
            .setRegParam(regParam)
            .setElasticNetParam(elasticNetParam)

        // Fit the model
        val lrModel = lr.fit(data)

        // Print the coefficients and intercept for linear regression
        println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

        // Summarize the model over the training set and print out some metrics
        val trainingSummary = lrModel.summary
        println(s"numIterations: ${trainingSummary.totalIterations}")
        println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
        trainingSummary.predictions.show()
        trainingSummary.residuals.show()
        val out = trainingSummary.predictions.select("label", "prediction").collect().map { x => (x(0), x(1), (x(0).asInstanceOf[Double] - x(1).asInstanceOf[Double])).toString().replace("(", "").replace(")", "") }
        println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
        println(s"r2: ${trainingSummary.r2}")

        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> outputFile)), out);
        //    S3Dispatcher.dispatch(null, Map("filePath" -> "src/test/resources/device-recos-training/RETest/RFR-testData.txt", "bucket" -> "lpdev-ekstep", "key" -> "Data-Sciences/explore/poc/SparkML/LR-predictions.txt"))  
    }

}