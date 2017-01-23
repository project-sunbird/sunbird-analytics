package org.ekstep.analytics.views

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf

class TestPreComputedViews extends SparkSpec(null) {
    
    "PrecomputedViews" should "output views to a file" in {
        
       val dispatchParams = JSONUtils.deserialize[Map[String, AnyRef]](AppConf.getConfig("pc_dispatch_params"));
        val data = sc.parallelize(List(""))
        PrecomputedViews.execute(data, Some(dispatchParams))
    }
  
}