package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector

class TestREScoringModel extends SparkSpec(null) {

    "REScoringModel" should "load model and generate scores" in {

//        val jobParams1 = Map("model" -> "fm.model.test1", "localPath" -> "src/test/resources/RE-scoring-model/")
        val me1 = REScoringModel.execute(null, None)

        val jobParams2 = Map("model" -> "fm.model.test2", "localPath" -> "src/test/resources/RE-scoring-model/")
        val me2 = REScoringModel.execute(null, Option(jobParams2))

        val jobParams3 = Map("model" -> "fm.model.test3", "localPath" -> "src/test/resources/RE-scoring-model/")
        val me3 = REScoringModel.execute(null, Option(jobParams3))

        val jobParams4 = Map("model" -> "fm.model.test4", "localPath" -> "src/test/resources/RE-scoring-model/")
        val me4 = REScoringModel.execute(null, Option(jobParams4))
    }

}