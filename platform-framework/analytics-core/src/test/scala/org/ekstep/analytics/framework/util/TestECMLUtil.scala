package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec

class TestECMLUtil extends BaseSpec {
    it should "pass test case of all methods in ECMLUtil" in {
        // getAssetIds
        val string = "{ \"theme\": { \"id\": \"theme\", \"version\": \"1.0\", \"startStage\": \"5c1a1618e80d40848964429546576eae\", \"manifest\": { \"media\": [ { \"ver\": \"1.0\", \"src\": \"plugin.js\", \"id\": \"org.ekstep.quiz\", \"type\": \"plugin\" }, { \"asset_id\": \"flying_cap2\", \"type\": \"image\", \"src\": \"cap_flying/fc1.png\" }, { \"assetId\": \"flying_cap3\", \"type\": \"image\", \"src\": \"cap_flying/fc1.png\" } ] }, \"controller\": { \"name\": \"assessment\", \"type\": \"items\", \"id\": \"assessment\" } } }"
        val media = ECMLUtil.getAssetIds(string)
        media(0) should be("org.ekstep.quiz")
        media(1) should be("flying_cap2")
        media(2) should be("flying_cap3")
        
        // getAssetIds with empty theme 
        val string1 = "{ \"theme\": {} }"
        val media1 = ECMLUtil.getAssetIds(string1)
        media1.length should be(0)
        
        // getAssetIds with empty manifest
        val string2 = "{ \"theme\": { \"id\": \"theme\", \"version\": \"1.0\", \"startStage\": \"5c1a1618e80d40848964429546576eae\", \"manifest\": {}, \"controller\": { \"name\": \"assessment\", \"type\": \"items\", \"id\": \"assessment\" } } }"
        val media2 = ECMLUtil.getAssetIds(string2)
        media2.length should be(0)
        
        // getAssetIds with empty media
        val string3 = "{ \"theme\": { \"id\": \"theme\", \"version\": \"1.0\", \"startStage\": \"5c1a1618e80d40848964429546576eae\", \"manifest\": { \"media\": [] }, \"controller\": { \"name\": \"assessment\", \"type\": \"items\", \"id\": \"assessment\" } } }"
        val media3 = ECMLUtil.getAssetIds(string3)
        media3.length should be(0)
    }
}