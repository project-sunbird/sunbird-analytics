package org.ekstep.analytics.adapter

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
class TestDomainAdapter extends BaseSpec {

    ignore should "return domain map" in {
    //"DomainAdapter" should "return domain map" in {
        val domain = DomainAdapter.getDomainMap();
        domain should not be null;
        domain.concepts should not be null;
        domain.relations should not be null;
        domain.concepts.length should be > 0;
        //domain.relations.length should be > 0; TODO: We have to check with LP team 
        val objectTypes = domain.concepts.map(f => f.metadata.get("objectType").get).distinct;
        objectTypes should contain("Domain")
        objectTypes should contain("Dimension")
        objectTypes should contain("Concept")
    }
    
}