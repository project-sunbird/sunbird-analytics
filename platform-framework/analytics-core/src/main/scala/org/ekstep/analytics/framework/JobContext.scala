package org.ekstep.analytics.framework

/**
 * @author Santhosh
 */
object JobContext {
  
    var parallelization:Int = 10;
    
    var deviceMapping: Map[String, String] = Map();
    
    var jobName: String = "default";
}