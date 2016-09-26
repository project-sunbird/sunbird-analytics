package org.ekstep.analytics.transformer

import org.apache.spark.rdd.RDD
import breeze.stats._
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

trait RETransformer[T, R] {
  
    def name() : String = "RETransformer";
    
    def binning(rdd: RDD[(String, Double)], numBuckets: Int)(implicit sqlContext: SQLContext): RDD[(String, String)] = {

        val rows = rdd.map(f => Row.fromSeq(Seq(f._1, f._2)));
        val structs = new StructType(Array(new StructField("key", StringType, true), new StructField("value", DoubleType, true)));
        val df = sqlContext.createDataFrame(rows, structs);
        val discretizer = new QuantileDiscretizer()
            .setInputCol("value")
            .setOutputCol("n_value")
            .setNumBuckets(numBuckets)

        val result = discretizer.fit(df).transform(df).drop("value").rdd;
        result.map { x => (x.getString(0), x.getDouble(1).toString()) };
    }
    
    def outlierTreatment(rdd: RDD[(String, Double)]): RDD[(String, Double)] = {
        val valueArray = rdd.map { x => x._2 }.collect()
        val q1 = DescriptiveStats.percentile(valueArray, 0.25)
        //val q2 = DescriptiveStats.percentile(valueRDD, 0.5)
        val q3 = DescriptiveStats.percentile(valueArray, 0.75)
        val iqr = q3 - q1
        val lowerLimit = q1 - (1.5 * iqr)
        val upperLimit = q3 + (1.5 * iqr)
        println(lowerLimit + " " + upperLimit)
        rdd.map { x =>
            if (x._2 < lowerLimit) (x._1, lowerLimit)
            else if (x._2 > upperLimit) (x._1, upperLimit)
            else x
        }
    }
    
    def getTransformationByBinning(rdd: RDD[T])(implicit sc: SparkContext): RDD[(String, R)]
    
    def removeOutliers(rdd: RDD[T])(implicit sc: SparkContext): RDD[T]
}