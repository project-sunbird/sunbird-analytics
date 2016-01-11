package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import java.io.FileWriter

/**
 * @author Santhosh
 */
object FileDispatcher extends IDispatcher {

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

}