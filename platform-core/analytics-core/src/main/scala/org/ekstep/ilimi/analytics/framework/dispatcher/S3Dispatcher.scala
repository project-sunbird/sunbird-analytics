package org.ekstep.ilimi.analytics.framework.dispatcher

import org.ekstep.ilimi.analytics.framework.exception.DispatcherException
import org.ekstep.ilimi.analytics.framework.util.S3Util
import java.io.FileWriter
import org.ekstep.ilimi.analytics.framework.conf.AppConf

/**
 * @author Santhosh
 */
object S3Dispatcher extends IDispatcher {
    
    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]) : Array[String] = {
        val filePath = config.getOrElse("filePath", null).asInstanceOf[String];
        val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
        val key = config.getOrElse("key", null).asInstanceOf[String];
        val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];
        
        if (null == bucket || null == key) {
            throw new DispatcherException("'bucket' & 'key' parameters are required to send output to S3");
        }
        
        if (null != filePath) {
            outputToS3(bucket, isPublic, key, filePath);
        } else {
            outputToS3(bucket, isPublic, key, events)
        }
        events;
    }

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
        events.foreach { x => { fw.write(x + "\n"); } };
        fw.close();
        if (isPublic) {
            S3Util.uploadPublic(bucket, tempFilePath, key);
        } else {
            S3Util.upload(bucket, tempFilePath, key);
        }
    }

}