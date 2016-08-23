package org.ekstep.analytics.adapter

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.exception.DataAdapterException

/**
 * @author Santhosh
 */
class TestContentAdapter extends BaseSpec {
  
    "ContentAdapter" should "return content list using v2 api" in {
        
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
    
    it should "return content items using v2 api" in {
        
        val contentItems = ContentAdapter.getContentItems("org.akshara.worksheet1");
        contentItems should not be (null);
        contentItems.length should be (0);
        
        // Invoke getContentWrapper and getItemWrapper for code coverage
        ContentAdapter.getContentWrapper(Map[String, AnyRef]("identifier" -> "c123"));
        ContentAdapter.getItemWrapper(Map[String, AnyRef]("identifier" -> "i123"));
    }
    
    it should "get live contents form content model" in {
        ContentAdapter.getLiveContent(5).length should be (5)
    }
}