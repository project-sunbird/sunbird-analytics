package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec

/**
 * @author Santhosh
 */
class TestS3Util extends BaseSpec {
    
    "S3Util" should "search keys in S3 bucket" in {
        
        val keys = S3Util.search("lpdev-ekstep", "output/test", None, None, None);
        keys.length should be > 0;
        
        val keys2 = S3Util.search("sandbox-ekstep-telemetry", "sandbox.telemetry.unique-", None, None, Option(2));
        keys2 should not be null;
    }
  
}