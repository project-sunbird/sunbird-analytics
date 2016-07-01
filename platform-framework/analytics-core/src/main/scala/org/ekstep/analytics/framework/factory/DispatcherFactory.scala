package org.ekstep.analytics.framework.factory

import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.dispatcher.IDispatcher
import org.ekstep.analytics.framework.dispatcher.ConsoleDispatcher
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.dispatcher.FileDispatcher
import org.ekstep.analytics.framework.util.JobLogger


/**
 * @author Santhosh
 */
object DispatcherFactory {

    val className = "org.ekstep.analytics.framework.factory.DispatcherFactory"
    
    @throws(classOf[DispatcherException])
    def getDispatcher(disp: Dispatcher): IDispatcher = {
        disp.to.toLowerCase() match {
            case "s3"      =>
                S3Dispatcher;
            case "kafka"   =>
                KafkaDispatcher;
            case "script"  =>
                ScriptDispatcher;
            case "console" =>
                ConsoleDispatcher;
            case "file" =>
                FileDispatcher;
            case _         =>
                val msg = "Unknown output dispatcher destination found"
                val exp = new DispatcherException(msg)
                JobLogger.log(msg, className, Option(exp), None, Option("FAILED"), "ERROR")
                throw exp;
        }
    }
}