package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework.BaseSpec

/**
 * @author Santhosh
 */
class TestUserAdapter extends BaseSpec {
     
    "UserAdapter" should "return users demographics" in {
        val userMapping = UserAdapter.getUserMapping();
        userMapping.size should be > 50;
    }
    
    it should "return language mappings" in {
        val langMapping = UserAdapter.getLanguageMapping()
        langMapping.size should be >= 3;
    }
}