package org.ekstep.analytics.framework.dispatcher

/**
 * @author Santhosh
 */
object ConsoleDispatcher extends IDispatcher {

    def dispatch(events: Array[String], config: Map[String, AnyRef]): Array[String] = {
        if (config.getOrElse("printEvent", true).asInstanceOf[Boolean]) {
            for (event <- events) {
                println("Event", event);
            }
        }
        println("Total Events Size", events.length);
        events;
    }
}