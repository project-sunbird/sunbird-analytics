package org.ekstep.ilimi.analytics.framework.dispatcher

import org.ekstep.ilimi.analytics.framework.exception.DispatcherException
import org.ekstep.ilimi.analytics.framework.util.S3Util
import java.io.FileWriter
import org.ekstep.ilimi.analytics.framework.conf.AppConf

/**
 * @author Santhosh
 */
object S3Dispatcher {

    @throws(classOf[DispatcherException])
    def outputToS3(bucket: String, isPublic: Boolean, key: String, filePath: String) = {
        if (isPublic) {
            S3Util.uploadPublic(bucket, filePath, key);
        } else {
            S3Util.upload(bucket, filePath, key);
        }
    }

    @throws(classOf[DispatcherException])
    def outputToS3(bucket: String, isPublic: Boolean, key: String, events: Array[String]) = {
        // Create temp file
        val tempFilePath = AppConf.getConfig("spark_output_temp_dir") + "/output-" + System.currentTimeMillis();
        val fw = new FileWriter(tempFilePath, true);
        events.foreach { x => { fw.write(x); } };
        fw.close();
        if (isPublic) {
            S3Util.uploadPublic(bucket, tempFilePath, key);
        } else {
            S3Util.upload(bucket, tempFilePath, key);
        }
    }

}