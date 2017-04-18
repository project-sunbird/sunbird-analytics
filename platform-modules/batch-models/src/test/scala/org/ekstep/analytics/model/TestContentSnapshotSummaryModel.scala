package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.framework.util.JSONUtils

class TestContentSnapshotSummaryModel extends SparkGraphSpec(null) {
  
    "ContentSnapshotSummaryModel" should "generate content snapshot summary events" in {

        // Running all VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));
        
        val rdd = ContentSnapshotSummaryModel.execute(sc.makeRDD(List(Empty())), None);
    }
}