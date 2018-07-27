package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JSONUtils
import java.io.File
import org.ekstep.analytics.framework.util.CommonUtil
import java.io.IOException
import org.apache.kafka.common.errors.TimeoutException


/**
 * @author Santhosh
 */
class TestOutputDispatcher extends SparkSpec("src/test/resources/sample_telemetry_2.log") {
  
    "OutputDispatcher" should "dispatch output to console" in {
        
        val outputs = Option(Array(
            Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef]))        
        ))
        val eventsInString = events.map{x => JSONUtils.serialize(x)}
        noException should be thrownBy {
            OutputDispatcher.dispatch(outputs, eventsInString);
        }
        
        noException should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("console", Map()), sc.parallelize(events.take(1)));
        }
        
        val eventsInArray = events.map{x => JSONUtils.serialize(x)}.collect
        noException should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("console", Map()), eventsInArray);
        }
    }
    
    it should "dispatch output to s3" in {
        val output1 = Dispatcher("s3file", Map[String, AnyRef]("bucket" -> "ekstep-dev-data-store", "key" -> "output/test-log1.json", "zip" -> true.asInstanceOf[AnyRef]));
        val output2 = Dispatcher("s3file", Map[String, AnyRef]("bucket" -> "ekstep-dev-data-store", "key" -> "output/test-log2.json", "filePath" -> "src/test/resources/sample_telemetry.log"));
        noException should be thrownBy {
            OutputDispatcher.dispatch(output1, events);
            OutputDispatcher.dispatch(output2, events);
        }
    }
    
    it should "throw dispatcher exceptions" in {
        
        // Unknown Dispatcher
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("xyz", Map[String, AnyRef]()), events);
        }
        
        // Invoke kafka dispatcher with no parameters
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("kafka", Map[String, AnyRef]()), events);
        }
        
        // Invoke kafka dispatcher with missing topic
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> "localhost:9092")), events);
        }
        
        // Invoke kafka dispatcher without starting kafka
//        OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> "localhost:9092", "topic" -> "test")), events);	
        
        // Invoke script dispatcher without required fields ('script')     
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("script", Map[String, AnyRef]()), events);
        }
        
        // Invoke File dispatcher without required fields ('file')
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("file", Map[String, AnyRef]()), events);
        }
        
        // Invoke script dispatcher with invalid script
        a[IOException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "src/test/resources/simpleScript3.sh")), events);
        }
        
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "src/test/resources/simpleScript2.sh")), events);
        }
        
        // Invoke S3 dispatcher without required fields ('bucket','key')
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("s3", Map[String, AnyRef]("zip" -> true.asInstanceOf[AnyRef])), events);
        }
        
        // Invoke dispatch with null dispatcher
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(null.asInstanceOf[Dispatcher], events);
        }
        
        val eventsInArray = events.map{x => JSONUtils.serialize(x)}.collect
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(null.asInstanceOf[Dispatcher], eventsInArray);
        }
        
        // Invoke dispatch with None dispatchers
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(None, events);
        }
        
        val noEvents = sc.parallelize(Array[Event]());
        
        // Invoke dispatch with Empty events
        //a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])), noEvents);
        //}
        
        // Invoke dispatch with Empty events
        //a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), noEvents);
        //}
            
        OutputDispatcher.dispatch(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])), Array[String]());
    }
    
    it should "execute test cases related to script dispatcher" in {
        
        val result = OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "src/test/resources/simpleScript.sh")), events);
        //result(0) should endWith ("analytics-core");
        //result(1) should include ("7436");
    }
    
    
    it should "dispatch output to a file" in {
        
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/test_output.log")), events);
        val f = new File("src/test/resources/test_output.log");
        f.exists() should be (true)
        CommonUtil.deleteFile("src/test/resources/test_output.log");
    }

    it should "dispatch output to azure" in {
        val output1 = Dispatcher("azure", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> "output/test-dispatcher1.json", "zip" -> true.asInstanceOf[AnyRef]));
        val output2 = Dispatcher("azure", Map[String, AnyRef]("bucket" -> "dev-data-store", "key" -> "output/test-dispatcher2.json", "filePath" -> "src/test/resources/sample_telemetry.log"));
        noException should be thrownBy {
            OutputDispatcher.dispatch(output1, events);
            OutputDispatcher.dispatch(output2, events);
        }
    }
    
}