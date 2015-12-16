package org.ekstep.ilimi.analytics.framework

import org.ekstep.ilimi.analytics.framework.exception.DispatcherException
import org.ekstep.ilimi.analytics.framework.util.JSONUtils


/**
 * @author Santhosh
 */
class TestOutputDispatcher extends SparkSpec {
  
    "OutputDispatcher" should "dispatch output to console" in {
        val outputs = Option(Array(
            Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef]))        
        ))
        noException should be thrownBy {
            OutputDispatcher.dispatch(outputs, events.map { x => JSONUtils.serialize(x) });
        }
    }
    
    it should "dispatch output to s3" in {
        val outputs = Option(Array(
            Dispatcher("s3", Map[String, AnyRef]("bucket" -> "lpdev-ekstep", "key" -> "output/akshara-log.json", "zip" -> true.asInstanceOf[AnyRef]))
        ))
        noException should be thrownBy {
            OutputDispatcher.dispatch(outputs, events.map { x => JSONUtils.serialize(x) });
        }
    }
    
}