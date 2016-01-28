package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework.DomainMap
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.framework.util.Constants
import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.DomainResponse
import org.ekstep.analytics.framework.DomainMap
import org.ekstep.analytics.framework.Concept
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
object DomainAdapter {
    
    val relations = Array("tags");
    
    def getDomainMap() : DomainMap = {
        val dr = RestUtil.get[DomainResponse](Constants.getDomainMap);
        if (!dr.responseCode.equals("OK")) {
            throw new DataAdapterException(dr.responseCode);
        }
        if(dr.result.concepts == null) {
            throw new DataAdapterException("No concepts found in the domain graph");
        }
        val concepts = dr.result.concepts.map(f => {
            getConceptWrapper(f);
        });
        DomainMap(concepts, dr.result.relations);
    }
    
    private def getConceptWrapper(concept: Map[String, AnyRef]): Concept = {
        Concept(concept.get("identifier").get.asInstanceOf[String], concept.filterNot(p => relations.contains(p._1)), CommonUtil.getTags(concept));
    }
  
}