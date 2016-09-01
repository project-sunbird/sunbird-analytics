package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import java.io.FileWriter
import org.ekstep.analytics.framework.OutputDispatcher
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.commons.lang3.StringUtils

/**
 * @author Santhosh
 */
object FileDispatcher extends IDispatcher {

    implicit val className = "org.ekstep.analytics.framework.dispatcher.FileDispatcher";

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]): Array[String] = {
        val filePath = config.getOrElse("file", null).asInstanceOf[String];
        if (null == filePath) {
            throw new DispatcherException("'file' parameter is required to send output to file");
        }
        val fw = new FileWriter(filePath, true);
        events.foreach { x => { fw.write(x + "\n"); } };
        fw.close();
        events;
    }

    @throws(classOf[DispatcherException])
    def dispatchDF(events: Array[String], config: Map[String, AnyRef], header: String): Array[String] = {
        val filePath = config.getOrElse("file", null).asInstanceOf[String];
        if (null == filePath) {
            throw new DispatcherException("'file' parameter is required to send output to file");
        }
        val fw = new FileWriter(filePath, true);
        fw.write(header + "\n")
        events.foreach { x => { fw.write(x + "\n"); } };
        fw.close();
        events;
    }

    @throws(classOf[DispatcherException])
    def dispatchRETrainingFile[T](dispatcherMap: Map[String, String], events: RDD[T]) = {

        if (null == dispatcherMap) {
            throw new DispatcherException("No output configurations found");
        }

        val filePath1 = dispatcherMap.getOrElse("file1", null);
        val filePath2 = dispatcherMap.getOrElse("file2", null);

        if (null == filePath1 || null == filePath2) {
            throw new DispatcherException("'file' parameter is required to send output to file");
        }

        val eventArr = OutputDispatcher.stringify(events).collect();

        if (eventArr.length != 0) {
            JobLogger.log("Dispatching output", Option(dispatcherMap));
            val fw1 = new FileWriter(filePath1, true);
            val fw2 = new FileWriter(filePath2, true);
            eventArr.foreach { x =>
                if (!StringUtils.startsWith(x, "0.0"))
                    fw1.write(x + "\n");
                fw2.write(x + "\n");
            };
            fw1.close();
            fw2.close();
        } else {
            JobLogger.log("No events produced");
            null;
        }
    }

}