package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JSONUtils
import java.io.File
import org.ekstep.analytics.framework.util.CommonUtil
import java.io.IOException


/**
 * @author Santhosh
 */
class TestOutputDispatcher extends SparkSpec {
  
    "OutputDispatcher" should "dispatch output to console" in {
        
        val outputs = Option(Array(
            Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef]))        
        ))
        noException should be thrownBy {
            OutputDispatcher.dispatch(outputs, meEvent);
        }
        
        noException should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("console", Map()), sc.parallelize(meEvent.take(1)));
        }
    }
    
    it should "dispatch output to s3" in {
        val output1 = Dispatcher("s3", Map[String, AnyRef]("bucket" -> "lpdev-ekstep", "key" -> "output/test-log1.json", "zip" -> true.asInstanceOf[AnyRef]));
        val output2 = Dispatcher("s3", Map[String, AnyRef]("bucket" -> "lpdev-ekstep", "key" -> "output/test-log2.json", "filePath" -> "src/test/resources/sample_telemetry.log"));
        noException should be thrownBy {
            OutputDispatcher.dispatch(output1, meEvent);
            OutputDispatcher.dispatch(output2, meEvent);
        }
    }
    
    it should "throw dispatcher exceptions" in {
        
        // Unknown Dispatcher
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("xyz", Map[String, AnyRef]()), meEvent);
        }
        
        // Invoke kafka dispatcher with no parameters
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("kafka", Map[String, AnyRef]()), meEvent);
        }
        
        // Invoke kafka dispatcher with missing topic
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> "localhost:9092")), meEvent);
        }
        
        // Invoke kafka dispatcher without starting kafka
        OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> "localhost:9092", "topic" -> "test")), meEvent);
        
        // Invoke script dispatcher without required fields ('script')     
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("script", Map[String, AnyRef]()), meEvent);
        }
        
        // Invoke File dispatcher without required fields ('file')
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("file", Map[String, AnyRef]()), meEvent);
        }
        
        // Invoke script dispatcher with invalid script
        a[IOException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "src/test/resources/simpleScript3.sh")), meEvent);
        }
        
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "src/test/resources/simpleScript2.sh")), meEvent);
        }
        
        // Invoke S3 dispatcher without required fields ('bucket','key')
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("s3", Map[String, AnyRef]("zip" -> true.asInstanceOf[AnyRef])), meEvent);
        }
        
        // Invoke dispatch with null dispatcher
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(null.asInstanceOf[Dispatcher], meEvent);
        }
        
        // Invoke dispatch with None dispatchers
        a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(None, meEvent);
        }
        
        val noEvents = sc.parallelize(Array[MEEvent]());
        
        // Invoke dispatch with Empty events
        //a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])), noEvents);
        //}
        
        // Invoke dispatch with Empty events
        //a[DispatcherException] should be thrownBy {
            OutputDispatcher.dispatch(Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), noEvents);
        //}
    }
    
    it should "execute test cases related to script dispatcher" in {
        
        val result = OutputDispatcher.dispatch(Dispatcher("script", Map("script" -> "src/test/resources/simpleScript.sh")), meEvent);
        result(0) should endWith ("analytics-core");
        result(1) should include ("7436");
    }
    
    
    it should "dispatch output to a file" in {
        
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/test_output.log")), meEvent);
        val f = new File("src/test/resources/test_output.log");
        f.exists() should be (true)
        CommonUtil.deleteFile("src/test/resources/test_output.log");
    }
    
}