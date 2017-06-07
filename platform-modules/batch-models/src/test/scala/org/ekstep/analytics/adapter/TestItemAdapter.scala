package org.ekstep.analytics.adapter

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.exception.DataAdapterException

/**
 * @author Santhosh
 */
class TestItemAdapter extends BaseSpec {
    
    "ItemAdapter" should "return item concepts and max score" in {
        
        val item2 = ItemAdapter.getItemConceptMaxScore("numeracy_382", "q_3_s_gujarati", "v2");
        item2 should not be (null);
        item2.concepts should be (Array("LO50"));
        item2.maxScore should be (1);
        
        ItemAdapter.getItemWrapper(Map[String, AnyRef]("identifier" -> "i123"));
        ItemAdapter.getItemWrapper(Map[String, AnyRef]("identifier" -> "ek.n.q935 - QA", "concepts" -> List(Map("identifier" -> "Num:C1:SC1", "objectType" -> "Concept"))));
    }
    
}