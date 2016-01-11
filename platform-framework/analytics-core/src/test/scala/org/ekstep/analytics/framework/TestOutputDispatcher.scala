package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JSONUtils


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
        
        noException should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("console", Map()), sc.parallelize(events.map { x => JSONUtils.serialize(x) }.take(1)));
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
    
    it should "throw unknown dispatcher exception" in {
        
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("xyz", Map[String, AnyRef]()), events.map { x => JSONUtils.serialize(x) });
        }
    }
    
    it should "throw dispatcher exception while invoking kafka dispatcher" in {
        
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("kafka", Map[String, AnyRef]()), events.map { x => JSONUtils.serialize(x) });
        }
    }
    
    it should "throw dispatcher exception while invoking script dispatcher" in {
        
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("script", Map[String, AnyRef]()), events.map { x => JSONUtils.serialize(x) });
        }
    }
    
    it should "throw dispatcher exception while invoking file dispatcher" in {
        
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("file", Map[String, AnyRef]()), events.map { x => JSONUtils.serialize(x) });
        }
    }
    
}