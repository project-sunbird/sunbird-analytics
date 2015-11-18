package org.ekstep.ilimi.analytics.framework.dispatcher

/**
 * @author Santhosh
 */
object ConsoleDispatcher extends IDispatcher {

    def dispatch(events: Array[String], config: Map[String, AnyRef]) : Array[String] = {
        for(event <- events) {
            Console.println("Event", event);
        }
        Console.println("Total Events Size", events.length);
        events;
    }
}