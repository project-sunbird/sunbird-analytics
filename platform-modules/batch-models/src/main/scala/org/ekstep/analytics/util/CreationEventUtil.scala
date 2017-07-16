package org.ekstep.analytics.util

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.creation.model.CreationPData
import org.ekstep.analytics.framework.ETags
import org.apache.commons.lang3.StringUtils

//import org.ekstep.analytics.creation.model.ETags

object CreationEventUtil {
  
    def getEventSyncTS(event: CreationEvent): Long = {
        val timeInString = event.`@timestamp`;
        CommonUtil.getEventSyncTS(timeInString);
    }
    
    def getAppDetails(event: CreationEvent): CreationPData = {
        if (event.pdata.nonEmpty && StringUtils.isNotBlank(event.pdata.get.id)) event.pdata.get else new CreationPData(AppConf.getConfig("default.creation.app.id"), "1.0")
    }
    
    def getChannelId(event: CreationEvent): String = {
        if (event.channel.nonEmpty && StringUtils.isNotBlank(event.channel.get)) event.channel.get else AppConf.getConfig("default.channel.id")
    }
    
    def getETags(event: CreationEvent): ETags = {
        if(event.etags.isDefined){
                event.etags.get
        }else {
               ETags()
        }
    }
}