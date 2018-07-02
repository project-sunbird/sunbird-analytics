package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.model.SparkGraphSpec
import org.scalatest.Ignore

@Ignore
class TestContentSnapshotSummarizer extends SparkGraphSpec(null) {
  
    it should "execute ContentSnapshotSummarizer job and won't throw any Exception" in {
        
        // Running all dependent VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));
        
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))))), null, None, "org.ekstep.analytics.model.ContentSnapshotSummary", Option(Map("active_user_days_limit" -> 30.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestContentSnapshotSummarizer"))
        ContentSnapshotSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}