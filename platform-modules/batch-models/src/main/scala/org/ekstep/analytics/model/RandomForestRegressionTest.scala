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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher

object RandomForestRegressionTest extends App {

    val sc = CommonUtil.getSparkContext(2, "test")
    implicit val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
        
    // Load and parse the data file, converting it to a DataFrame.
    val input = sc.textFile("src/test/resources/device-recos-training/RETest/train.dat.libfm")
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
        LabeledPoint(label,Vectors.sparse(vecSize+1, indices.toArray, values.toArray))
    }
    val data = x.toDF()
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 6 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(6)
        .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val train = trainingData.rdd
    val test = testData.rdd
//    OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/device-recos-training/RETest/trainData1.txt")), train);
//    OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/device-recos-training/RETest/testData.txt")), test);
    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
        .setLabelCol("label")
        .setFeaturesCol("indexedFeatures")

    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
        .setStages(Array(featureIndexer, rf))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(trainingData)

//    predictions.select("prediction", "label", "features", "indexedFeatures").rdd.saveAsTextFile("src/test/resources/device-recos-training/RETest/predictions")
    predictions.show()
    // Select example rows to display.
    val out = predictions.select("prediction", "label").collect().map{x => (x(0), x(1), (x(0).asInstanceOf[Double]-x(1).asInstanceOf[Double])).toString().replace("(", "").replace(")", "")}//.show(20)
//    OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/device-recos-training/RETest/predictions1.txt")), out);
    
    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
//    println("Learned regression forest model:\n" + rfModel.toDebugString)
}