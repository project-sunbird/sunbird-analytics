package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.S3Util
import java.io.FileWriter
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JobLogger

/**
 * @author Santhosh
 */
object S3Dispatcher extends IDispatcher {
    
    val className = "org.ekstep.analytics.framework.dispatcher.S3Dispatcher"
    
    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]) : Array[String] = {
        var filePath = config.getOrElse("filePath", null).asInstanceOf[String];
        val bucket = config.getOrElse("bucket", null).asInstanceOf[String];
        val key = config.getOrElse("key", null).asInstanceOf[String];
        val zip = config.getOrElse("zip", false).asInstanceOf[Boolean];
        val isPublic = config.getOrElse("public", false).asInstanceOf[Boolean];
        
        if (null == bucket || null == key) {
            val msg = "'bucket' & 'key' parameters are required to send output to S3"
            val exp = new DispatcherException(msg)
            JobLogger.log(msg, className, Option(exp), None, Option("FAILED"), "ERROR")
            throw exp;
        }
        var deleteFile = false;
        if (null == filePath) {
            filePath = AppConf.getConfig("spark_output_temp_dir") + "output-" + System.currentTimeMillis() + ".log";
            val fw = new FileWriter(filePath, true);
            events.foreach { x => { fw.write(x + "\n"); } };
            fw.close();
            deleteFile = true;
        } 
        val finalPath = if (zip) CommonUtil.gzip(filePath) else filePath;
        S3Util.upload(bucket, finalPath, key);
        if(deleteFile) CommonUtil.deleteFile(filePath);
        if (zip) CommonUtil.deleteFile(finalPath);
        events;
    }

}