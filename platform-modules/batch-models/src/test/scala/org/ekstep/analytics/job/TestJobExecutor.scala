package org.ekstep.analytics.job

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.exception.JobNotFoundException
import com.fasterxml.jackson.databind.JsonMappingException

class TestJobExecutor extends BaseSpec {

    "JobExecutor" should "execute job when model code is passed" in {
        a[JsonMappingException] should be thrownBy {
            JobExecutor.main("ss", "");
        }

        the[JobNotFoundException] thrownBy {
            JobExecutor.main("abc", null);
        } should have message "Unknown job type found"
    }

}