package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import breeze.stats._
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{ CountVectorizerModel, CountVectorizer, RegexTokenizer }
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._

trait DeviceRecommendationTransformer[T, R] {

    def name(): String = "DeviceRecommendationTransformer";
    
    def binning(rdd: RDD[(String, Double)], numBuckets: Int)(implicit sqlContext: SQLContext): RDD[(String, Double)] = {

        val rows = rdd.map(f => Row.fromSeq(Seq(f._1, f._2)));
        val structs = new StructType(Array(new StructField("key", StringType, true), new StructField("value", DoubleType, true)));
        val df = sqlContext.createDataFrame(rows, structs);
        val discretizer = new QuantileDiscretizer()
            .setInputCol("value")
            .setOutputCol("n_value")
            .setNumBuckets(numBuckets)

        val result = discretizer.fit(df).transform(df).drop("value").rdd;
        result.map { x => (x.getString(0), x.getDouble(1)) };
    }

    def getTransformationByBinning(rdd: RDD[T])(implicit sc: SparkContext): RDD[(String, R)]    

    private def flattenColumn(df: DataFrame, colToSplit: String, cols: Array[String])(implicit sc: SparkContext): DataFrame = {

        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        if (cols.isEmpty) df
        else {
            flattenColumn(
                df.select(df.columns.map(df(_)): _*).join(df.select($"label", $"colToSplit", $"colToSplit".getItem(cols.length - 1).cast("double").as(colToSplit + "_" + cols(cols.length - 1))).drop("colToSplit"), "label"),
                colToSplit,
                cols.slice(0, cols.length - 1))
        }
    }

    def oneHotEncoding(in: DataFrame, colName: String)(implicit sc: SparkContext): DataFrame = {

        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        val tokenizer = new RegexTokenizer().setInputCol(colName).setOutputCol("words").setPattern("\\w+").setGaps(false)
        val wordsData = tokenizer.transform(in)
        val cvModel: CountVectorizerModel = new CountVectorizer()
            .setInputCol("words")
            .setOutputCol("features")
            .fit(wordsData)

        val possibleValues = cvModel.vocabulary
        val out = cvModel.transform(wordsData)
        val asArray = udf((v: Vector) => v.toArray)
        val outDF = out.withColumn("featureArray", asArray(out.col("features"))).drop("features").drop("words")
        val x = outDF.withColumnRenamed("featureArray", "colToSplit").withColumnRenamed("c1_total_ts", "label")
        flattenColumn(x, colName, possibleValues).drop("colToSplit").drop(colName).withColumnRenamed("label", "c1_total_ts")
        
    }

}