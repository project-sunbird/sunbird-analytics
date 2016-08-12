package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec

/**
 * @author Santhosh
 */
class TestS3Util extends BaseSpec {
    
    "S3Util" should "search keys in S3 bucket" in {
        
        val keys = S3Util.search("lpdev-ekstep", "output/test", None, None, None);
        keys.length should be > 0;
        
        val keys2 = S3Util.search("sandbox-data-store", "raw/", None, Option("2016-01-03"), Option(2));
        keys2 should not be null;
    }
    
    "S3Util" should "download keys from S3 bucket" in {
        
        S3Util.download("lpdev-ekstep", "output/", "files/");
    }
  
    it should "upload multiple files into S3 bucket" in {
        S3Util.uploadDirectory("lpdev-ekstep", "testUpload/", "src/test/resources/session-batch-model")
        val keys = S3Util.search("lpdev-ekstep", "testUpload/", None, None, None);
        keys.length should be > 0;
        S3Util.uploadDirectory("lpdev-ekstep", "testUpload/", "src/test/resources/test")
    }
    
    it should "download multiple files from a directory in S3 bucket" in {
         S3Util.downloadFile("lpdev-ekstep", "test-data-session.log", "src/test/resources/session-batch-model", "testUpload/")
         S3Util.downloadFile("lpdev-ekstep", "test-data-session.log", "src/test/resources/testDir", "testUpload/")
    }
}