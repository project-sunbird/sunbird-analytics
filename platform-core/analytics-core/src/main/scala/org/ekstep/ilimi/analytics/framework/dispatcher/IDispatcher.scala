package org.ekstep.ilimi.analytics.framework.dispatcher

import org.ekstep.ilimi.analytics.framework.exception.DispatcherException

/**
 * @author Santhosh
 */
trait IDispatcher {

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]) : Array[String];
}