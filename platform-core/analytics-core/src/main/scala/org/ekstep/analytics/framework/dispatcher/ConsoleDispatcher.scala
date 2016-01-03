package org.ekstep.analytics.framework.dispatcher

/**
 * @author Santhosh
 */
object ConsoleDispatcher extends IDispatcher {

    def dispatch(events: Array[String], config: Map[String, AnyRef]): Array[String] = {
        if (config.getOrElse("printEvent", true).asInstanceOf[Boolean]) {
            for (event <- events) {
                Console.println("Event", event);
            }
        }
        if (config.getOrElse("printEventSize", true).asInstanceOf[Boolean]) {
            Console.println("Total Events Size", events.length);
        }
        events;
    }
}