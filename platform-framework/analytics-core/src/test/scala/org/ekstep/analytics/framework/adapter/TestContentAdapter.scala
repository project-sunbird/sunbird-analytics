package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework.BaseSpec
import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
class TestContentAdapter extends BaseSpec {
  
    "ContentAdapter" should "return game list using v1 API" in {
        
        val games = ContentAdapter.getGameList();
        games should not be (null);
        games.length should be > 0;
        
        val game = games.filter(f => "numeracy_382".equals(f.identifier))(0);
        game.identifier should be ("numeracy_382");
        game.subject should be ("numeracy");
        game.code should be ("org.ekstep.aser.lite");
        game.objectType should be ("Game");
    }
    
    it should "return content list using v2 api" in {
        
        val contents = ContentAdapter.getAllContent();
        contents should not be (null);
        contents.length should be > 0;
        val content = contents.filter(f => "numeracy_374".equals(f.id))(0);
        content.id should be ("numeracy_374");
        content.tags.get.length should be (0);
        content.concepts.length should be (0);
        
        val cm = content.metadata;
        cm.get("contentType").get should be ("Game");
        cm.get("code").get should be ("org.ekstep.quiz.app");
        cm.get("status").get should be ("Live");
        cm.get("name").get should be ("EkStep Content App");
        cm.get("owner").get should be ("EkStep");
        cm.get("mimeType").get should be ("application/vnd.android.package-archive");
    }
    
    it should "return content items using v1 api" in {
        
        val contentItems = ContentAdapter.getContentItems("numeracy_382");
        contentItems should not be (null);
        contentItems.length should be > 0;
        
        val item = contentItems.filter(x => "q_1_s_tamil".equals(x.id))(0);
        item.id should be ("q_1_s_tamil");
        item.mc.get.length should be (0);
        item.tags.get.length should be (0);
        item.metadata.get("code").get should be ("q_1_s_tamil");
        item.metadata.get("type").get should be ("recognition");
        item.metadata.get("max_score").get should be (1);
        item.metadata.get("gradeLevel").get should be (2);
        
        val item2 = contentItems.filter(x => "q_sub_q1020".equals(x.id))(0);
        item2.id should be ("q_sub_q1020");
        item2.mc.get should be (Array("Num:C3:SC1:MC12"));
        item2.tags.get.length should be (0);
        item2.metadata.get("code").get should be ("q_sub_q1020");
        item2.metadata.get("type").get should be ("ftb");
        item2.metadata.get("max_score").get should be (1);
        item2.metadata.get("gradeLevel").get should be (2);
        item2.metadata.get("qlevel").get should be ("MEDIUM");
        item2.metadata.get("num_answers").get should be (4);
    }
    
    it should "return content items using v2 api" in {
        
        val contentItems = ContentAdapter.getContentItems("org.akshara.worksheet1");
        contentItems should not be (null);
        contentItems.length should be (0);
        
        // Invoke getContentWrapper and getItemWrapper for code coverage
        ContentAdapter.getContentWrapper(Map[String, AnyRef]("identifier" -> "c123"));
        ContentAdapter.getItemWrapper(Map[String, AnyRef]("identifier" -> "i123"));
    }
}