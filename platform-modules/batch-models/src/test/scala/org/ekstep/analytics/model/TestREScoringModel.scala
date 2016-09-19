package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector

class TestREScoringModel extends SparkSpec(null) {

    "REScoringModel" should "load model and generate scores" in {

        val jobParams1 = Map("libFMTrainConfig" -> "-dim 1,1,10 -iter 100 -method sgd -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 100")
        DeviceRecommendationModel.execute(null, Option(jobParams1))
        val me1 = REScoringModel.execute(null, None)   
    }

    it should "load model with zero pairwise interactions and generate scores" in {
        
        val jobParams2 = Map("libFMTrainConfig" -> "-dim 1,1,0 -iter 100 -method sgd -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 100")
        DeviceRecommendationModel.execute(null, Option(jobParams2))
        val jobParams = Map("model" -> "fm.model", "localPath" -> "/tmp/")
        val me2 = REScoringModel.execute(null, Option(jobParams))
    }
    
    it should "load model with zero W0 and generate scores" in {
        
        val jobParams3 = Map("libFMTrainConfig" -> "-dim 0,1,5 -iter 100 -method sgd -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 100")
        DeviceRecommendationModel.execute(null, Option(jobParams3))
//        val jobParams3 = Map("model" -> "fm.model.test3", "localPath" -> "src/test/resources/RE-scoring-model/")
        val me3 = REScoringModel.execute(null, None)
    }
    
    it should "load model with zero unary interactions and generate scores" in {
        
        val jobParams4 = Map("libFMTrainConfig" -> "-dim 1,0,10 -iter 100 -method sgd -task r -regular 3,10,10 -learn_rate 0.01 -seed 100 -init_stdev 100")
        DeviceRecommendationModel.execute(null, Option(jobParams4))
//        val jobParams4 = Map("model" -> "fm.model.test4", "localPath" -> "src/test/resources/RE-scoring-model/")
        val me4 = REScoringModel.execute(null, None)
    }
}