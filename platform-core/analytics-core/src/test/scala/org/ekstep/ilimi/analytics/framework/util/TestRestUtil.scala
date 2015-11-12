package org.ekstep.ilimi.analytics.framework.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.ekstep.ilimi.analytics.framework.Response
import com.fasterxml.jackson.core.JsonParseException
import org.ekstep.ilimi.analytics.framework.Search
import org.ekstep.ilimi.analytics.framework.Request
import org.ekstep.ilimi.analytics.framework.Metadata
import org.ekstep.ilimi.analytics.framework.SearchFilter

/**
 * @author Santhosh
 */
class TestRestUtil extends FlatSpec with Matchers {

    "RestUtil.get()" should "get data from learning platform API and parse it to Response Object" in {
        val url = Constants.getContentAPIUrl("org.ekstep.story.hi.elephant");
        val response = RestUtil.get[Response](url);
        response should not be null;
        response.responseCode should be("OK")
    }

    it should "throw JsonParseException if unable to parse to Response object" in {
        val url = "https://www.google.com";
        a[JsonParseException] should be thrownBy {
            RestUtil.get[Response](url);
        }
    }
    
    it should "return error if the resource is not found" in {
        val url = Constants.getContentAPIUrl("xyz");
        val response = RestUtil.get[Response](url);
        response should not be null;
        response.responseCode should not be("OK")
    }
    
    "RestUtil.post()" should "post data to learning platform API and parse body to Response Object" in {
        val url = Constants.getSearchItemAPIUrl("numeracy");
        val search = Search(Request(Metadata(Array(SearchFilter("identifier", "in", Option(Array("ek.n.q901", "ek.n.q902", "ek.n.q903"))))), 500));
        val response = RestUtil.post[Response](url, JSONUtils.serialize(search));
        response should not be null;
        response.responseCode should be("OK")
    }

    it should "throw JsonParseException if unable to parse to Response object" in {
        val url = "https://www.google.com";
        a[JsonParseException] should be thrownBy {
            RestUtil.post[Response](url, "");
        }
    }
    
    it should "return error response if body is not passed" in {
        val url = Constants.getSearchItemAPIUrl("numeracy");
        a[JsonParseException] should be thrownBy {
            RestUtil.post[Response](url, "");
        }
    }
    
    it should "return success response even if no data found for search query" in {
        val url = Constants.getSearchItemAPIUrl("numeracy");
        val search = Search(Request(Metadata(Array(SearchFilter("identifier", "in", Option(Array("xyz1"))))), 500));
        val response = RestUtil.post[Response](url, JSONUtils.serialize(search));
        response should not be null;
        response.responseCode should be("OK")
    }

}