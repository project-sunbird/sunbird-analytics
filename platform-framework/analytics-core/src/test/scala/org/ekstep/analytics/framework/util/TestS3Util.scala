package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec

/**
 * @author Santhosh
 */
class TestS3Util extends BaseSpec {
    
    "S3Util" should "search keys in S3 bucket" in {
        
        val keys = S3Util.search("ekstep-dev-data-store", "output/test", None, None, None);
        keys.length should be > 0;
        
        val keys2 = S3Util.search("ekstep-dev-data-store", "raw/", None, Option("2016-01-03"), Option(2));
        keys2 should not be null;
    }
    
    "S3Util" should "download keys from S3 bucket" in {
        
        S3Util.download("ekstep-dev-data-store", "output/", "files/");
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
         S3Util.downloadFile("sandbox-data-store", "model/fm.model", "src/test/resources/testDir/")
         CommonUtil.deleteDirectory("src/test/resources/testDir/")
    }
    
    it should "get object from S3 bucket" in {
        S3Util.getObject("sandbox-data-store", "model/fm.model")
        val res = S3Util.getObject("sandbox-data-store", "fm.model")
        res.length should be (0)
    }
}