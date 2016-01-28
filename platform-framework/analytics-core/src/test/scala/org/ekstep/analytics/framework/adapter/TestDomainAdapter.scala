package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework.BaseSpec
import org.ekstep.analytics.framework.exception.DataAdapterException

/**
 * @author Santhosh
 */
class TestDomainAdapter extends BaseSpec {

    "DomainAdapter" should "return domain map" in {
        val domain = DomainAdapter.getDomainMap();
        domain should not be null;
        domain.concepts should not be null;
        domain.relations should not be null;
        domain.concepts.length should be > 0;
        domain.relations.length should be > 0;
        val objectTypes = domain.concepts.map(f => f.metadata.get("objectType").get).distinct;
        objectTypes should contain("Domain")
        objectTypes should contain("Dimension")
        objectTypes should contain("Concept")
    }
}