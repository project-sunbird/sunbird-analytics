package org.ekstep.analytics.framework.factory

import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.dispatcher.IDispatcher
import org.ekstep.analytics.framework.dispatcher.ConsoleDispatcher
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.dispatcher.FileDispatcher


/**
 * @author Santhosh
 */
object DispatcherFactory {

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
                throw new DispatcherException("Unknown output dispatcher destination found");
        }
    }
}