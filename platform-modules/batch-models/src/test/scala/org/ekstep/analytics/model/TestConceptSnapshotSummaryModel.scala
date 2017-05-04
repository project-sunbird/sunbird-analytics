package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

class TestConceptSnapshotSummaryModel extends SparkGraphSpec(null) {
  
    "ConnceptSnapshotSummaryModel" should "generate concept snapshot summary events" in {

        // Running all VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));
        
        val rdd = ConceptSnapshotSummaryModel.execute(sc.makeRDD(List()), None);
        val events = rdd.collect
        events.length should be(1)

        val event1 = events(0);
        
        event1.context.pdata.model should be("ConceptSnapshotSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SNAPSHOT");
        event1.context.date_range should not be null;
        event1.dimensions.concept_id.get should be("Num:C1:SC1")

        val eks1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks1.get("live_content_count").get should be(1)
        eks1.get("total_content_count").get should be(1)
        eks1.get("review_content_count").get should be(0)
        
    }
    
    it should "generate concept snapshot summary events with all fields 0" in {

        // Running all VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));
        
        val query = "CREATE (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept', IL_UNIQUE_ID:'test_concept', contentCount: 0, liveContentCount: 0}) RETURN cnc"
        GraphQueryDispatcher.dispatch(query)
        
        val rdd = ConceptSnapshotSummaryModel.execute(sc.makeRDD(List()), None);
        val events = rdd.collect
        events.length should be(2)

        val event1 = events(1);
        
        event1.context.pdata.model should be("ConceptSnapshotSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SNAPSHOT");
        event1.context.date_range should not be null;
        event1.dimensions.concept_id.get should be("test_concept")

        val eks1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks1.get("live_content_count").get should be(0)
        eks1.get("total_content_count").get should be(0)
        eks1.get("review_content_count").get should be(0)
        
    }
}