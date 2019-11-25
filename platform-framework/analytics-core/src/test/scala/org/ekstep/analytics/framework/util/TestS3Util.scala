package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec
import org.jets3t.service.S3ServiceException
import org.jets3t.service.ServiceException

/**
 * @author Santhosh
 */
class TestS3Util extends BaseSpec {
    
    ignore should "search keys in S3 bucket" in {
        
        val keys = S3Util.search("ekstep-dev-data-store", "output/test", None, None, None);
        keys.length should be > 0;
        
        val keys2 = S3Util.search("ekstep-dev-data-store", "raw/", None, Option("2016-01-03"), Option(2));
        keys2 should not be null;
    }

    ignore should "download keys from S3 bucket" in {
        
        S3Util.download("ekstep-dev-data-store", "output/", "files/");
    }

    ignore should "upload multiple files into S3 bucket" in {
        S3Util.uploadDirectory("ekstep-dev-data-store", "testUpload/", "src/test/resources/session-batch-model")
        val keys = S3Util.search("ekstep-dev-data-store", "testUpload/", None, None, None);
        keys.length should be > 0;
        S3Util.uploadDirectory("ekstep-dev-data-store", "testUpload/", "src/test/resources/test")
    }

    ignore should "download multiple files from a directory in S3 bucket" in {
         S3Util.downloadFile("ekstep-dev-data-store", "test-data-session.log", "src/test/resources/session-batch-model", "testUpload/")
         CommonUtil.createDirectory("src/test/resources/testDir/")
         S3Util.downloadFile("ekstep-dev-data-store", "test-data-session.log", "src/test/resources/testDir", "testUpload/")
         S3Util.downloadFile("ekstep-dev-data-store", "model/fm.model", "src/test/resources/testDir/")
         CommonUtil.deleteDirectory("src/test/resources/testDir/")
    }

    ignore should "get object from S3 bucket" in {
        S3Util.getObject("ekstep-dev-data-store", "model/fm.model")
        val res = S3Util.getObject("ekstep-dev-data-store", "fm.model")
        res.length should be (0)
    }

    ignore should "check uploadPublic with & without expiry into s3" in {
        S3Util.uploadPublic("ekstep-dev-data-store", "src/test/resources/sample_telemetry.log", "testUpload/")
        S3Util.uploadPublicWithExpiry("ekstep-dev-data-store", "src/test/resources/sample_telemetry.log", "testUpload/", 2)
    }

    ignore should "check getObjectDetails method" in {
        S3Util.getObjectDetails("ekstep-dev-data-store", "testUpload/test-data-launch.log")
//        a[S3ServiceException] should be thrownBy {
//            S3Util.getObjectDetails("ekstep-dev-data-store", "testUpload1/test-data-launch.log")
//        }
    }

    ignore should "check deleteObject method" in {
        val keys = S3Util.search("ekstep-dev-data-store", "testUpload/test-data-launch.log", None, None, None);
        keys.length should be > 0;  
        S3Util.deleteObject("ekstep-dev-data-store", "testUpload/test-data-launch.log")
        val keysAfter = S3Util.search("ekstep-dev-data-store", "testUpload/test-data-launch.log", None, None, None);
        keysAfter.length should be (0);
        
//        S3Util.deleteObject("ekstep-dev-data-store", "testUpload/test-data-launch-test.log")
    }
}