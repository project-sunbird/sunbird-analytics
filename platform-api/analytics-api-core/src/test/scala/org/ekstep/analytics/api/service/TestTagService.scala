package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.util.DBUtil
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.util.JSONUtils
import org.joda.time.DateTimeUtils

class TestTagService extends BaseSpec {
	
	val tagId = "e3618fe64fe05916346e5494d0a8e24ab36d7bfd";
	
	override def beforeAll() {
        super.beforeAll()
        DateTimeUtils.setCurrentMillisFixed(1464859204280L); // Fix the date-time to be returned by DateTime.now()
    }
	
	override def afterAll() {
        // Cleanup test data
        val query = "DELETE FROM " + Constants.CONTENT_DB + "." + Constants.REGISTERED_TAGS + " where tag_id='"+tagId+"'"
        DBUtil.session.execute(query);
        super.afterAll();
    }
  
	"TagService" should "register given tag" in {
		val response = TagService.registerTag(tagId);
		val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.tag-register");
        resp.params.status should be (Some("successful"));
        resp.ts should be ("2016-06-02T09:20:04.280+00:00");
	}
	
	ignore should "un-register given tag" in {
		val response = TagService.deleteTag(tagId);
		val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.tag-delete");
        resp.params.status should be (Some("successful"));
        resp.ts should be ("2016-06-02T09:20:04.280+00:00");
	}
}