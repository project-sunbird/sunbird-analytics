package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.mllib.linalg.DenseVector

class TestDeviceRecommendationScoringModel extends SparkSpec(null) {

    //    "REScoringModel" should "load model and generate scores" in {
    //
    //        DeviceRecommendationModel.execute(null, None)
    //        REScoringModel.execute(null, None)
    //
    //    }
    //
    //    it should "load model with zero pairwise interactions and generate scores" in {
    //        
    //        val jobParams2 = Map("libFMTrainConfig" -> "-dim 1,1,10 -iter 100 -method sgd -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 100")
    //        DeviceRecommendationModel.execute(null, Option(jobParams2))
    //        val jobParams1 = Map("model" -> "fm.model", "localPath" -> "/tmp/")
    //        val me2 = REScoringModel.execute(null, Option(jobParams1))
    //    }
    //    
    //    it should "load model with zero W0 and generate scores" in {
    //        
    //        val jobParams3 = Map("libFMTrainConfig" -> "-dim 0,1,5 -iter 100 -method sgd -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 100")
    //        DeviceRecommendationModel.execute(null, Option(jobParams3))
    //        val me3 = REScoringModel.execute(null, None)
    //    }
    //    
    //    it should "load model with zero unary interactions and generate scores" in {
    //        
    //        val jobParams4 = Map("libFMTrainConfig" -> "-dim 1,0,10 -iter 100 -method sgd -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 100")
    //        DeviceRecommendationModel.execute(null, Option(jobParams4))
    //        val me4 = REScoringModel.execute(null, None)
    //    }

    it should "run scoringAlgo() method and generate scores" in {

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val x = List(new DenseVector(Array(0.0, 10.0, 0.0, 4.0, 1.0, 12.0)), new DenseVector(Array(1.0, 0.0, 4.0, 0.0, 3.0, 2.0)), new DenseVector(Array(0.0, 8.0, 0.0, 4.0, 1.0, 9.0)), new DenseVector(Array(1.0, 0.0, 0.0, 3.0, 1.0, 2.0)))
        val dataRDD = sc.parallelize(x)
        val output = DeviceRecommendationScoringModel.scoringAlgo(dataRDD, "src/test/resources/re-scoring/", "fm1.model").collect()
        output.map(x => println(x))
    }
}