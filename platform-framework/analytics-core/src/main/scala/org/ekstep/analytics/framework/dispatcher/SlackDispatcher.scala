package org.ekstep.analytics.framework.dispatcher

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JSONUtils


case class SlackMessage(channel: String, username: String, text: String, icon_emoji:String = ":ghost:")
/**
 * @author Santhosh
 */
object SlackDispatcher extends IDispatcher {

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]): Array[String] = {
        
        val channel = config.getOrElse("channel", null).asInstanceOf[String];
        val userName = config.getOrElse("userName", null).asInstanceOf[String];
        

        if (null == channel || null == userName) {
            throw new DispatcherException("'channel' & 'userName' parameters are required to send output to slack")
        }
        
        val text = events.mkString(",");
        val message = SlackMessage(channel, userName, text);
        val resp = RestUtil.post[String]("https://hooks.slack.com/services/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", JSONUtils.serialize(message));
        events;
    }
    
    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext) = {
        dispatch(events.collect(), config);
    }
    
}