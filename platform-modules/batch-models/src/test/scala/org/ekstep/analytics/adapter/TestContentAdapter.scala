package org.ekstep.analytics.adapter

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.BaseSpec
import org.scalamock.scalatest.MockFactory

/**
 * @author Santhosh
 */
class TestContentAdapter extends BaseSpec with MockFactory{
  
    ignore should "return content list using v2 api" in {
        
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
    
    ignore should "return content items using v2 api" in {
        
        val contentItems = ContentAdapter.getContentItems("org.akshara.worksheet1");
        contentItems should not be (null);
        contentItems.length should be (0);
        
        // Invoke getContentWrapper and getItemWrapper for code coverage
        ContentAdapter.getContentWrapper(Map[String, AnyRef]("identifier" -> "c123"));
        ContentAdapter.getItemWrapper(Map[String, AnyRef]("identifier" -> "i123"));
    }
    
    ignore should "get published contents form content model" in {
        val contents = ContentAdapter.getPublishedContent();
        println("count", contents.size, "sample", contents.head);
    }

    "Search method" should "handle when result count 0" in {
        trait util {
            def action(offset: Int, limit: Int): ContentResult
        }
        val mockAction = mock[util]

        val responseString = "{\"id\":\"ekstep.composite-search.search\",\"ver\":\"3.0\",\"ts\":\"2018-12-07T06:04:22ZZ\",\"params\":{\"resmsgid\":\"78d1afa1-32b0-47f1-96d8-a7372c1ee959\",\"msgid\":null,\"err\":null,\"status\":\"successful\",\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"count\":0}}"
        val response = JSONUtils.deserialize[ContentResponse](responseString)

        (mockAction.action _).expects(0, 200).returns(response.result)

        val result = ContentAdapter.search(0, 200, Array[Map[String, AnyRef]](), mockAction.action)
        result.length should be(0)
    }

    it should "return the result when content is found" in {
        trait util {
            def action(offset: Int, limit: Int): ContentResult
        }
        val mockAction = mock[util]

        val responseString = "{\"id\":\"ekstep.composite-search.search\",\"ver\":\"3.0\",\"ts\":\"2018-12-07T05:39:19ZZ\",\"params\":{\"resmsgid\":\"6512453e-a5af-4079-a9e0-2d4f79b8abcd\",\"msgid\":null,\"err\":null,\"status\":\"successful\",\"errmsg\":null},\"responseCode\":\"OK\",\"result\":{\"count\":1,\"content\":[{\"identifier\":\"do_1126053858324398081174\",\"objectType\":\"Content\"}]}}"
        val response = JSONUtils.deserialize[ContentResponse](responseString)

        (mockAction.action _).expects(0, 200).returns(response.result)

        val result = ContentAdapter.search(0, 200, Array[Map[String, AnyRef]](), mockAction.action)

        result.length should be(1)
        result.head.getOrElse("identifier", "").asInstanceOf[String] should be("do_1126053858324398081174")
    }
}