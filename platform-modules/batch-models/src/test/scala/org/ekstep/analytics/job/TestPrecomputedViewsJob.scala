package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.util.CommonUtil
import java.io.File

class TestPrecomputedViewsJob extends BaseSpec {
  
    it should "execute the PrecomputedViews successfully" in {
        PrecomputedViewsJob.main()
        val file = new File("/tmp/data-sets/")
        if(file.exists()){
            CommonUtil.deleteDirectory("/tmp/data-sets/"); 
        }
    }
}