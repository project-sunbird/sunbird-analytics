package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.SparkSpec
import com.datastax.spark.connector._
import org.ekstep.analytics.model.ModelParam

object UpdateProficiencyModelParam extends SparkSpec {

    def getModelParam(learner_id: String, mc: String): Map[String, Double] = {
        val row = sc.cassandraTable("learner_db", "proficiency_params").select("alpha", "beta").where("learner_id", learner_id).where("concept", mc).first()
        if (row.length == 0)
            Map("alpha" -> 0.5d, "beta" -> 1d);
        else
            Map("alpha" -> row.getDouble("alpha"), "beta" -> row.getDouble("beta"))
    }
    def saveModelParam(modelParams: List[ModelParam]){
        println(modelParams)
        sc.parallelize(modelParams, 1).saveAsCassandraTable("learner_db", "proficiencyparams", SomeColumns("learner_id", "concept", "alpha", "beta"))
    }
}