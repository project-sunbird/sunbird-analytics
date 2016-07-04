package org.ekstep.analytics.adapter

import org.ekstep.analytics.framework.DomainMap
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.framework.DomainResponse
import org.ekstep.analytics.framework.DomainMap
import org.ekstep.analytics.framework.Concept
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants

/**
 * @author Santhosh
 */
object DomainAdapter extends BaseAdapter {
    
    val relations = Array("tags");
    
    def getDomainMap() : DomainMap = {
        val dr = RestUtil.get[DomainResponse](Constants.getDomainMap);
        checkResponse(dr);
        val concepts = dr.result.concepts.map(f => {
            getConceptWrapper(f);
        });
        DomainMap(concepts, dr.result.relations);
    }
    
    private def getConceptWrapper(concept: Map[String, AnyRef]): Concept = {
        Concept(concept.get("identifier").get.asInstanceOf[String], concept.filterNot(p => relations.contains(p._1)), CommonUtil.getTags(concept));
    }
  
}