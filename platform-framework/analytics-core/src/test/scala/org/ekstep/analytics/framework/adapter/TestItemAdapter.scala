package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework.BaseSpec
import org.ekstep.analytics.framework.exception.DataAdapterException

/**
 * @author Santhosh
 */
class TestItemAdapter extends BaseSpec {
  
    "ItemAdapter" should "return Item object" in {
        val item = ItemAdapter.getItem("LT1.Q2", "literacy_v2");
        item should not be null;
        item.mc should not be None;
    }
    
    it should "return DataAdapterException when item is not found" in {
        a[DataAdapterException] should be thrownBy {
            ItemAdapter.getItem("item1", "xyz");
        }
    }
    
    it should "return Questionnaires" in {
        val questionnaires = ItemAdapter.getQuestionnaires("numeracy_377");
        questionnaires should not be(null);
    }
    
    it should "return DataAdapterException when content is not found" in {
        a[DataAdapterException] should be thrownBy {
            ItemAdapter.getQuestionnaires("zyx");
        }
        
        a[DataAdapterException] should be thrownBy {
            ItemAdapter.getItems("xyz");
        }
        
        a[DataAdapterException] should be thrownBy {
            ItemAdapter.getItemSets("xyz");
        }
    }
    
    it should "return all items" in {
        val items = ItemAdapter.getItems("numeracy_377");
        items should not be(null);
        items.length should be (30);
    }
    
    it should "be able to search item" in {
        val items = ItemAdapter.searchItems(Array("ek.n.q901", "ek.n.q903"), "numeracy");
        items should not be(null);
        items.length should be (2);
    }
    
    it should "return item set" in {
        val itemSet = ItemAdapter.getItemSet("numeracy_413", "numeracy");
        itemSet should not be(null);
        itemSet.items.length should be (15);
    }
    
    it should "return DataAdapterException when item set is not found" in {
        a[DataAdapterException] should be thrownBy {
            ItemAdapter.getItemSet("itemset_1", "numeracy");
        }
    }
    
    it should "return DataAdapterException when questinnaire is not found" in {
        a[DataAdapterException] should be thrownBy {
            ItemAdapter.getQuestionnaire("xyz", "abc");
        }
    }
    
    it should "return item sets" in {
        val itemSets = ItemAdapter.getItemSets("numeracy_377");
        itemSets should not be(null);
        itemSets.length should be (2);
        itemSets(0).items.length should be (15);
    }
    
    it should "return item concepts and max score" in {
        val item1 = ItemAdapter.getItemConceptMaxScore("numeracy_382", "q_3_s_gujarati", "v1");
        item1 should not be (null);
        val concepts = item1.concepts
        concepts should not be (null);
        concepts.asInstanceOf[Array[String]].length should be (1);
        concepts.asInstanceOf[Array[String]](0) should be ("LO50");
        item1.maxScore should be (1);
        
        val item2 = ItemAdapter.getItemConceptMaxScore("numeracy_382", "q_3_s_gujarati", "v2");
        item2 should not be (null);
        item2.concepts should be (null);
        item2.maxScore should be (1);
        
        ItemAdapter.getItemWrapper(Map[String, AnyRef]("identifier" -> "i123"));
    }
}