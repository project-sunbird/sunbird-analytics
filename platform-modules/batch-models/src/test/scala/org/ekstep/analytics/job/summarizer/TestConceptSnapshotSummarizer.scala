package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.model.SparkGraphSpec

class TestConceptSnapshotSummarizer extends SparkGraphSpec(null) {
  
    it should "execute ConceptSnapshotSummarizer job and won't throw any Exception" in {
        
        // Running all dependent VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));
        
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))), null, None, "org.ekstep.analytics.model.ConceptSnapshotSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestConceptSnapshotSummarizer"))
        ConceptSnapshotSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}