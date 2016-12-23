package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.BaseSpec

/**
 * @author Santhosh
 */
class TestConsoleDispatcher extends BaseSpec {
    
    "ConsoleDispatcher" should "send output to console" in {
        
        val events = ConsoleDispatcher.dispatch(Array[String]("test"), Map[String, AnyRef]());
        events.length should be (1);
        events(0) should be ("test");
    }
  
}