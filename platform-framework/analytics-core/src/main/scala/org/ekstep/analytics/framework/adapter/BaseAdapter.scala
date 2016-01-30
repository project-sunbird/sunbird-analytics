package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.DomainResponse

/**
 * @author Santhosh
 */
trait BaseAdapter {

    @throws(classOf[DataAdapterException])
    def checkResponse(resp: Response) = {
        
        if (!resp.responseCode.equals("OK")) {
            throw new DataAdapterException(resp.responseCode);
        }
    }
    
    def checkResponse(resp: DomainResponse) = {
        
        if (!resp.responseCode.equals("OK")) {
            throw new DataAdapterException(resp.responseCode);
        }
    }

}