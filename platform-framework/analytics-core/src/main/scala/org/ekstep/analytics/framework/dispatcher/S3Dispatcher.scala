package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.S3Util
import java.io.FileWriter
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
object S3Dispatcher extends IDispatcher {
    
    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]) : Array[String] = {
        var filePath = config.getOrElse("filePath", null).asInstanceOf[String];
        val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
        val key = config.getOrElse("key", null).asInstanceOf[String];
        val zip = config.getOrElse("zip", false).asInstanceOf[Boolean];
        val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];
        
        if (null == bucket || null == key) {
            throw new DispatcherException("'bucket' & 'key' parameters are required to send output to S3");
        }
        
        if (null == filePath) {
            filePath = AppConf.getConfig("spark_output_temp_dir") + "output-" + System.currentTimeMillis() + ".log";
            val fw = new FileWriter(filePath, true);
            events.foreach { x => { fw.write(x + "\n"); } };
            fw.close();
        }
        val finalPath = if (zip) CommonUtil.gzip(filePath) else filePath;
        if (isPublic) {
            S3Util.uploadPublic(bucket, finalPath, key);
        } else {
            S3Util.upload(bucket, finalPath, key);
        }
        CommonUtil.deleteFile(finalPath);
        if (zip) CommonUtil.deleteFile(filePath);
        events;
    }

}