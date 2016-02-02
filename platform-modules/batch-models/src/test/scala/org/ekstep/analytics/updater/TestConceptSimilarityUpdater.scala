package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.ConceptSimilarityJson
import org.ekstep.analytics.updater.ConceptSimilarityUpdater.ConceptSimilarity
import com.datastax.spark.connector._

class TestConceptSimilarityUpdater extends SparkSpec(null) {

    "ConceptSimilarityUpdater" should "write concept similarity data to db" in {
        val rdd1 = loadFile[ConceptSimilarityJson]("src/test/resources/concept-similarity/ConceptSimilarity.json");
        ConceptSimilarityUpdater.execute(sc, rdd1, Option(Map("modelVersion" -> "1.0", "modelId" -> "ConceptSimilarityUpdater")));
        val rowRDD = sc.cassandraTable[ConceptSimilarity]("learner_db", "conceptsimilaritymatrix");
        rowRDD.count() should not be (0);
    }
}