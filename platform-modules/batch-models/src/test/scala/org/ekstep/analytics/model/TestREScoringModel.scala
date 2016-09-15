package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector

class TestREScoringModel extends SparkSpec(null) {
  
    ignore should "load model and generate scores" in {

        val jobParams1 = Map("model" -> "src/test/resources/RE-scoring-model/fm.model.test1")
        REScoringModel.execute(null, Option(jobParams1))
        
//        val jobParams2 = Map("model" -> "src/test/resources/RE-scoring-model/fm.model.test2")
//        val me2 = REScoringModel.execute(null, Option(jobParams2))
//        
//        val jobParams3 = Map("model" -> "src/test/resources/RE-scoring-model/fm.model.test3")
//        val me3 = REScoringModel.execute(null, Option(jobParams3))
    }

}