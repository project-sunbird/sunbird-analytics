package org.ekstep.analytics.util

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.V3Event

/**
 * @author Santhosh
 */
trait SessionBatchModel[T, R] extends IBatchModel[T, R] {

    def getGameSessionsV3(data: RDD[V3Event]): RDD[((String, String), Buffer[V3Event])] = {
        data.filter { x => x.actor.id != null && x.`object`.nonEmpty && x.`object`.get.id != null }
            .map { event =>
                val channelId = CommonUtil.getChannelId(event)
                ((channelId, event.actor.id), Buffer(event))
            }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                events.sortBy { x => x.ets }.groupBy { e =>
                    (e.context.sid.get, e.ver)
                }.mapValues { x =>
                    var sessions = Buffer[Buffer[V3Event]]();
                    var tmpArr = Buffer[V3Event]();
                    var lastContentId: String = x(0).`object`.get.id;
                    x.foreach { y =>
                        y.eid match {
                            case "START" =>
                                if (tmpArr.length > 0) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[V3Event]();
                                }
                                tmpArr += y;
                                lastContentId = y.actor.id;
                            case "END" =>
                                tmpArr += y;
                                sessions += tmpArr;
                                tmpArr = Buffer[V3Event]();
                            case _ =>
                                if (!lastContentId.equals(y.actor.id)) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[V3Event]();
                                }
                                tmpArr += y;
                                lastContentId = y.actor.id;
                        }
                    }
                    sessions += tmpArr;
                    sessions;
                }.map(f => f._2).reduce((a, b) => a ++ b);
                //}.flatMap(f => f._2.map { x => ((f._1._2, f._1._1), x) }).filter(f => f._2.nonEmpty);
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }

    def getGameSessions(data: RDD[Event]): RDD[((String, String), Buffer[Event])] = {
        data.filter { x => x.uid != null && x.gdata.id != null }
            .map { event =>
                val channelId = CommonUtil.getChannelId(event)
                ((channelId, event.uid), Buffer(event))
            }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                events.sortBy { x => CommonUtil.getEventTS(x) }.groupBy { e =>
                    (e.sid, e.ver)
                }.mapValues { x =>
                    var sessions = Buffer[Buffer[Event]]();
                    var tmpArr = Buffer[Event]();
                    var lastContentId: String = x(0).gdata.id;
                    x.foreach { y =>
                        y.eid match {
                            case "OE_START" =>
                                if (tmpArr.length > 0) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                }
                                tmpArr += y;
                                lastContentId = y.gdata.id;
                            case "OE_END" =>
                                tmpArr += y;
                                sessions += tmpArr;
                                tmpArr = Buffer[Event]();
                            case _ =>
                                if (!lastContentId.equals(y.gdata.id)) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                }
                                tmpArr += y;
                                lastContentId = y.gdata.id;
                        }
                    }
                    sessions += tmpArr;
                    sessions;
                }.map(f => f._2).reduce((a, b) => a ++ b);
                //}.flatMap(f => f._2.map { x => ((f._1._2, f._1._1), x) }).filter(f => f._2.nonEmpty);
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }

    def getGenieLaunchSessions(data: RDD[Event], idleTime: Int): RDD[((String, String), Buffer[Event])] = {
        data.filter { x => x.did != null }.map { event =>
            val channelId = CommonUtil.getChannelId(event)
            ((channelId, event.did), Buffer(event))
        }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                val sortedEvents = events.sortBy { x => CommonUtil.getEventTS(x) }
                var sessions = Buffer[Buffer[Event]]();
                var tmpArr = Buffer[Event]();
                sortedEvents.foreach { y =>
                    y.eid match {
                        case "GE_GENIE_START" | "GE_START" =>
                            if (tmpArr.length > 0) {
                                sessions += tmpArr;
                                tmpArr = Buffer[Event]();
                            }
                            tmpArr += y;

                        case "GE_GENIE_END" | "GE_END" =>
                            if (!tmpArr.isEmpty) {
                                val event = tmpArr.last
                                val timeSpent = CommonUtil.getTimeDiff(CommonUtil.getEventTS(event), CommonUtil.getEventTS(y)).get
                                if (timeSpent > (idleTime * 60)) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                }
                            }
                            tmpArr += y;
                            sessions += tmpArr;
                            tmpArr = Buffer[Event]();
                        case _ =>
                            if (!tmpArr.isEmpty) {
                                val event = tmpArr.last
                                val timeSpent = CommonUtil.getTimeDiff(CommonUtil.getEventTS(event), CommonUtil.getEventTS(y)).get
                                if (timeSpent < (idleTime * 60)) {
                                    tmpArr += y;
                                } else {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                    tmpArr += y;
                                }
                            } else {
                                tmpArr += y;
                            }

                    }
                }
                sessions += tmpArr;
                sessions;
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }

    def getGenieSessions(data: RDD[Event], idleTime: Int): RDD[((String, String), Buffer[Event])] = {
        data.map { event =>
            val channelId = CommonUtil.getChannelId(event)
            ((channelId, event.sid), Buffer(event))
        }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                val sortedEvents = events.sortBy { x => CommonUtil.getEventTS(x) }
                var sessions = Buffer[Buffer[Event]]();
                var tmpArr = Buffer[Event]();
                sortedEvents.foreach { y =>
                    y.eid match {
                        case "GE_SESSION_START" =>
                            tmpArr += y;

                        case "GE_SESSION_END" =>
                            if (!tmpArr.isEmpty) {
                                val event = tmpArr.last
                                val timeSpent = CommonUtil.getTimeDiff(CommonUtil.getEventTS(event), CommonUtil.getEventTS(y)).get
                                if (timeSpent > (idleTime * 60)) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                }
                            }
                            tmpArr += y;
                            sessions += tmpArr;
                            tmpArr = Buffer[Event]();
                        case _ =>
                            if (!tmpArr.isEmpty) {
                                val event = tmpArr.last
                                val timeSpent = CommonUtil.getTimeDiff(CommonUtil.getEventTS(event), CommonUtil.getEventTS(y)).get
                                if (timeSpent < (idleTime * 60)) {
                                    tmpArr += y;
                                } else {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[Event]();
                                    tmpArr += y;
                                }
                            } else {
                                tmpArr += y;
                            }
                    }
                }
                sessions += tmpArr;
                sessions;
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }

    def getCESessions(data: RDD[CreationEvent]): RDD[((String, String), Buffer[CreationEvent])] = {
        data.filter { x => x.context.get.sid != null }
            .map { x =>
                val channelId = CreationEventUtil.getChannelId(x)
                ((channelId, x.context.get.sid), Buffer(x))
            }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                events.sortBy { x => x.ets }.groupBy { e => (e.context.get.content_id) }.mapValues { x =>
                    var sessions = Buffer[Buffer[CreationEvent]]();
                    var tmpArr = Buffer[CreationEvent]();
                    var lastContentId: String = x(0).context.get.content_id;
                    x.foreach { y =>
                        y.eid match {
                            case "CE_START" =>
                                if (tmpArr.length > 0) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[CreationEvent]();
                                }
                                tmpArr += y;
                                lastContentId = y.context.get.content_id;
                            case "CE_END" =>
                                tmpArr += y;
                                sessions += tmpArr;
                                tmpArr = Buffer[CreationEvent]();
                            case _ =>
                                if (!lastContentId.equals(y.context.get.content_id)) {
                                    sessions += tmpArr;
                                    tmpArr = Buffer[CreationEvent]();
                                }
                                tmpArr += y;
                                lastContentId = y.context.get.content_id;
                        }
                    }
                    sessions += tmpArr;
                    sessions;
                }.map(f => f._2).reduce((a, b) => a ++ b);
            }.flatMap(f => f._2.map { x => (f._1, x) }).filter(f => f._2.nonEmpty);
    }
}