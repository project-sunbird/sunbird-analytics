package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.job.consolidated.VidyavaaniModelJobs

class TestCreationRecommendationEnrichmentModel extends SparkGraphSpec(null) {

    it should "set confedence, lift value in the relation between Concept, Language & ContentType" in {

        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        VidyavaaniModelJobs.main("{}")(Option(sc));
        CreationRecommendationEnrichmentModel.main("{}")(Option(sc));
    }
}