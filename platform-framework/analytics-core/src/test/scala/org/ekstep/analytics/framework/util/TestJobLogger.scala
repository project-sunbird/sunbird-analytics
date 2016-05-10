package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec

class TestJobLogger extends BaseSpec {

    it should "pass test cases for all the methods in JobLogger" in {
        val jobName = "org.ekstep.analytics.framework.util.TestJobLogger"
        JobLogger.init(jobName);

        JobLogger.info("testing info method", jobName);
        JobLogger.debug("testing debug method", jobName);
        JobLogger.warn("testing warn method", jobName);
        JobLogger.error("testing error method", jobName, new Exception);
    }

}