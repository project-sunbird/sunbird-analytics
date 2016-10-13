package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.util.CommonUtil

class TestPrecomputedViewsJob extends BaseSpec {
  
    it should "execute the PrecomputedViews successfully" in {
        PrecomputedViewsJob.main()
        CommonUtil.deleteDirectory("/tmp/data-sets/");
    }
}