package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework.BaseSpec
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.DomainResponse

/**
 * @author Santhosh
 */

object SampleBaseAdapter extends BaseAdapter {
    
}

class TestBaseAdapter extends BaseSpec {
    
    "BaseAdapter" should "test all the methods" in {
        
        val resp = Response(null, null, null, null, "ERROR", null)
        a[DataAdapterException] should be thrownBy {
            SampleBaseAdapter.checkResponse(resp)
        }
        
        val domainResp = DomainResponse(null, null, null, null, "ERROR", null)
        a[DataAdapterException] should be thrownBy {
            SampleBaseAdapter.checkResponse(domainResp)
        }
        
    }
  
}