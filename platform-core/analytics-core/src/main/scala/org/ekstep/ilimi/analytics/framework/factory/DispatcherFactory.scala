package org.ekstep.ilimi.analytics.framework.factory

import org.ekstep.ilimi.analytics.framework.Dispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.IDispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.ConsoleDispatcher
import org.ekstep.ilimi.analytics.framework.exception.DispatcherException
import org.ekstep.ilimi.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.ScriptDispatcher


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
            case _         =>
                throw new DispatcherException("Unknown output dispatcher destination found");
        }
    }
}