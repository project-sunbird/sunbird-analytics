package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework.BaseSpec

/**
 * @author Santhosh
 */
class TestUserAdapter extends BaseSpec {
     
    "UserAdapter" should "return users demographics" in {
        val userMapping = UserAdapter.getUserProfileMapping();
        userMapping.size should be > 50;
        println(userMapping.last);
    }
    
    it should "return language mappings" in {
        val langMapping = UserAdapter.getLanguageMapping()
        langMapping.size should be >= 3;
    }
    
    it should "return user profile by id" in {
        val userProfile = UserAdapter.getUserProfileMapping("2b9f6758-bd43-443a-8382-a0566d7318ee");
        userProfile.gender should be ("male");
        userProfile.age should be (11);
        
        val up = UserAdapter.getUserProfileMapping("2b9f6758-bd43-443a-8382-");
        up should be (null);
    }
    
}