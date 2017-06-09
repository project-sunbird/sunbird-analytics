package org.ekstep.analytics.util

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.creation.model.CreationEvent

object CreationEventUtil {
  
    def getEventSyncTS(event: CreationEvent): Long = {
        val timeInString = event.`@timestamp`;
        CommonUtil.getEventSyncTS(timeInString);
    }
}