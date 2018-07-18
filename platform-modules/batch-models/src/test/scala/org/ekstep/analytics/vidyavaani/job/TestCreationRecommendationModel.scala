package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.GraphDBUtil
import org.scalatest.Ignore

@Ignore
class TestCreationRecommendationModel extends SparkGraphSpec(null) {
  
    it should "create recommendations for authors and populate the same into cassandra" in {

        // Running all VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));
        CreationRecommendationEnrichmentModel.main("{}")(Option(sc));
                        
        CreationRecommendationModel.main("{}")(Option(sc));
        
        val authorNodes = GraphQueryDispatcher.dispatch("MATCH(usr:User{type: 'author'}) RETURN usr").list;
        authorNodes.size() should be(2)
        
        // check for specific user with recommendations
        val table1 = sc.cassandraTable[RequestRecos](Constants.PLATFORM_KEY_SPACE_NAME, Constants.REQUEST_RECOS).where("uid=?", "290").first
        table1.requests.size should be(2)
        
        // check for specific user without recommendations
        val table2 = sc.cassandraTable[RequestRecos](Constants.PLATFORM_KEY_SPACE_NAME, Constants.REQUEST_RECOS).where("uid=?", "291")
        table2.count() should be(0)
        
    }
}