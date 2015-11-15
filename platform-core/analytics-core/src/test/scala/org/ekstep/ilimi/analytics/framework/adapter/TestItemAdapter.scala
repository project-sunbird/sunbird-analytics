package org.ekstep.ilimi.analytics.framework.adapter

import org.scalatest._

/**
 * @author Santhosh
 */
class TestItemAdapter extends FlatSpec with Matchers {
  
    "ItemAdapter" should "return Item object" in {
        val item = ItemAdapter.getItem("ek.n.q901", "numeracy");
        item should not be null;
        item.mc should not be None;
    }
    
    "ItemAdapter" should "return Questionnaires" in {
        val questionnaires = ItemAdapter.getQuestionnaires("org.ekstep.story.hi.elephant");
        Console.println("questionnaires", questionnaires);
        questionnaires should be(null);
    }
}