package org.ekstep.analytics.model

import scala.BigDecimal
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.ekstep.analytics.framework._
import org.ekstep.analytics.adapter._
import org.ekstep.analytics.framework.util._
import java.security.MessageDigest
import org.apache.log4j.Logger
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.SessionBatchModel
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.updater.LearnerProfile

/**
 * Case class to hold the item responses
 */
case class ItemResponse(itemId: String, itype: Option[AnyRef], ilevel: Option[AnyRef], timeSpent: Option[Double], exTimeSpent: Option[AnyRef], res: Option[Array[String]], resValues: Option[Array[AnyRef]], exRes: Option[AnyRef], incRes: Option[AnyRef], mc: Option[AnyRef], mmc: Option[AnyRef], score: Int, time_stamp: Long, maxScore: Option[AnyRef], domain: Option[AnyRef], pass: String, qtitle: Option[String], qdesc: Option[String]);

case class ActivitySummary(actType: String, count: Int, timeSpent: Double)
case class ScreenSummary(id: String, timeSpent: Double, visitCount: Long)
case class EventSummary(id: String, count: Int)
case class SessionSummaryInput(channel: String, userId: String, filteredEvents: Buffer[V3Event]) extends AlgoInput
case class SessionSummaryOutput(userId: String, appId: String, channel: String, ss: SessionSummary, groupInfo: Option[(Boolean, Boolean)]) extends AlgoOutput

/**
 * Case class to hold the screener summary
 */
class SessionSummary(val id: String, val ver: String, val noOfAttempts: Int, val timeSpent: Double,
                     val interruptTime: Double, val timeDiff: Double, val start_time: Long, val end_time: Long, val comments: Option[String], val fluency: Option[Int], val loc: Option[String],
                     val itemResponses: Option[Buffer[ItemResponse]], val dtRange: DtRange, val interactEventsPerMin: Double, val activitySummary: Option[Iterable[ActivitySummary]],
                     val completionStatus: Option[Boolean], val screenSummary: Option[Iterable[ScreenSummary]], val noOfInteractEvents: Int, val eventsSummary: Iterable[EventSummary],
                     val syncDate: Long, val contentType: Option[AnyRef], val mimeType: Option[AnyRef], val did: String, val mode: String, val etags: Option[ETags], val telemetryVer: String, val pdata: PData) extends Serializable {};

/**
 * @author Santhosh
 */

/**
 * Generic Screener Summary Model
 */
object LearnerSessionSummaryModel extends SessionBatchModel[V3Event, MeasuredEvent] with IBatchModelTemplate[V3Event, SessionSummaryInput, SessionSummaryOutput, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.LearnerSessionSummaryModel"

    override def name(): String = "LearnerSessionSummaryModel";
    
    val DEFAULT_MODE = "play";

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItem(itemMapping: Map[String, Item], event: V3Event): Item = {
        val item = itemMapping.getOrElse(event.edata.item.id, null);
        if (null != item) {
            return item;
        }
        return Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]()));
    }

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItemDomain(itemMapping: Map[String, Item], event: V3Event): String = {
        val item = itemMapping.getOrElse(event.edata.item.id, null);
        if (null != item) {
            return item.metadata.getOrElse("domain", "").asInstanceOf[String];
        }
        return "";
    }

    /**
     *
     */
    private def getItemData(contents: Array[Content], games: Array[String], apiVersion: String): Map[String, Item] = {

        val gameIds = contents.map { x => x.id };
        val codeIdMap: Map[String, String] = contents.map { x => (x.metadata.get("code").get.asInstanceOf[String], x.id) }.toMap;
        val contentItems = games.map { gameId =>
            {
                if (gameIds.contains(gameId)) {
                    (gameId, ContentAdapter.getContentItems(gameId, apiVersion))
                } else if (codeIdMap.contains(gameId)) {
                    (gameId, ContentAdapter.getContentItems(codeIdMap.get(gameId).get, apiVersion))
                } else {
                    null;
                }
            }
        }.filter(x => x != null).filter(_._2 != null).toMap;

        if (contentItems.size > 0) {
            contentItems.map(f => {
                f._2.map { item =>
                    (item.id, item)
                }
            }).reduce((a, b) => a ++ b).toMap;
        } else {
            Map[String, Item]();
        }
    }

    /**
     * Compute screen summaries on the telemetry data produced by content app
     */
    def computeScreenSummary(firstEvent: V3Event, lastEvent: V3Event, impressionEvents: Buffer[V3Event], interruptSummary: Map[String, Double]): Iterable[ScreenSummary] = {

        if (impressionEvents.length > 0) {
            var prevEvent = firstEvent;
            var stages = ListBuffer[(String, Double)]();
            impressionEvents.foreach { x =>
                val currStage = x.edata.pageid;
                val timeDiff = CommonUtil.getTimeDiff(prevEvent.ets, x.ets).get;
                stages += Tuple2(currStage, timeDiff);
                prevEvent = x;
            }

            // Last page screen summary, if lastEvent having pageid
            if (null != lastEvent.edata.pageid) {
                val timeDiff = CommonUtil.getTimeDiff(prevEvent.ets, lastEvent.ets).get;
                stages += Tuple2(lastEvent.edata.pageid, timeDiff);
            }

            stages.groupBy(f => f._1).mapValues(f => (f.map(x => x._2).sum, f.length)).map(f => {
                ScreenSummary(f._1, CommonUtil.roundDouble((f._2._1 - interruptSummary.getOrElse(f._1, 0d)), 2), f._2._2);
            });
        } else {
            Iterable[ScreenSummary]();
        }

    }
    def computeInterruptSummary(interruptEvents: Buffer[V3Event]): Map[String, Double] = {

        if (interruptEvents.length > 0) {
            interruptEvents.groupBy { event => event.context.pdata.get.id }.mapValues { events =>
                var prevTs: Long = 0;
                var ts: Double = 0;
                events.foreach { event =>
                    event.edata.`type` match {
                        case "RESUME" =>
                            if (prevTs == 0) prevTs = event.ets;
                            ts += CommonUtil.getTimeDiff(prevTs, event.ets).get;
                            prevTs == 0
                        case _ =>
                            prevTs = event.ets;
                    }
                }
                ts;
            }.toMap;
        } else {
            Map[String, Double]();
        }
    }

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[SessionSummaryInput] = {
        JobLogger.log("Filtering Events of ASSESS,START, END, LEVEL_SET, INTERACT, INTERRUPT")
        val filteredData = DataFilter.filter(data, Array(Filter("actor.id", "ISNOTEMPTY", None), Filter("eventId", "IN", Option(List("ASSESS", "START", "END", "INTERACT", "INTERRUPT", "IMPRESSION", "RESPONSE"))), Filter("context.env", "EQ", Option(Constants.PLAYER_ENV))));
        val gameSessions = getGameSessionsV3(filteredData);
        gameSessions.map { x => SessionSummaryInput(x._1._1, x._1._2, x._2) }
    }

    override def algorithm(data: RDD[SessionSummaryInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[SessionSummaryOutput] = {

        val events = data.map { x => x.filteredEvents }.flatMap { x => x }
        val gameList = events.map { x => x.`object`.get.id }.distinct().collect();
        JobLogger.log("Fetching the Content and Item data from Learning Platform")
        val contents = ContentAdapter.getAllContent();
        val itemData = getItemData(contents, gameList, "v2");
        JobLogger.log("Broadcasting data to all worker nodes")
        val catMapping = sc.broadcast(Map[String, String]("READING" -> "literacy", "MATH" -> "numeracy"));
        val deviceMapping = sc.broadcast(JobContext.deviceMapping);

        val itemMapping = sc.broadcast(itemData);
        val configMapping = sc.broadcast(config);
        JobLogger.log("Performing Game Sessionization")
        val contentTypeMap = contents.map { x => (x.id, (x.metadata.get("contentType"), x.metadata.get("mimeType"))) }
        val contentTypeMapping = sc.broadcast(contentTypeMap.toMap);
        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];

        JobLogger.log("Calculating Screen Summary")
        val screenerSummary = data.map { x =>

            val events = x.filteredEvents
            val firstEvent = events.head;
            val lastEvent = events.last;
            val telemetryVer = firstEvent.ver;
            val gameId = firstEvent.`object`.get.id;
            val gameVersion = firstEvent.`object`.get.ver.get;

            val content = contentTypeMapping.value.getOrElse(gameId, (Option("Game"), Option("application/vnd.android.package-archive")));
            val contentType = content._1;
            val mimeType = content._2;
            val assessEvents = events.filter { x => "ASSESS".equals(x.eid) }.sortBy { x => x.ets };
            val itemResponses = assessEvents.map { x =>
                val itemObj = getItem(itemMapping.value, x);
                val metadata = itemObj.metadata;
                val resValues = if (null == x.edata.resvalues) Option(Array[Map[String, AnyRef]]().map(f => f.asInstanceOf[AnyRef])) else Option(x.edata.resvalues.map(f => f.asInstanceOf[AnyRef]))
                val res = if (null == x.edata.resvalues) Option(Array[String]()); else Option(x.edata.resvalues.flatten.map { x => (x._1 + ":" + x._2.toString) });
                val item = x.edata.item
                ItemResponse(item.id, metadata.get("type"), metadata.get("qlevel"), Option(x.edata.duration) , Option(Int.box(item.exlength)), res, resValues, metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, Option(item.mmc), x.edata.score, x.ets, metadata.get("max_score"), metadata.get("domain"), x.edata.pass, Option(item.title), Option(item.desc));
            }
            val qids = assessEvents.map { x => x.edata.item.id }.filter { x => x != null };
            val qidMap = qids.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
            val noOfAttempts = if (qidMap.isEmpty) 1 else qidMap.max;
            val oeStarts = events.filter { x => "START".equals(x.eid) };
            val oeEnds = events.filter { x => "END".equals(x.eid) };
            val startTimestamp = firstEvent.ets;
            val endTimestamp = lastEvent.ets;
            val timeDiff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(startTimestamp, endTimestamp).get, 2);

            var tmpLastEvent: V3Event = null;
            val eventsWithTs = events.map { x =>
                if (tmpLastEvent == null) tmpLastEvent = x;
                val ts = CommonUtil.getTimeDiff(tmpLastEvent.ets, x.ets).get;
                tmpLastEvent = x;
                (x, if (ts > idleTime) 0 else ts)
            }

            val timeSpent = CommonUtil.roundDouble(eventsWithTs.map(f => f._2).sum, 2);

            val loc = deviceMapping.value.getOrElse(firstEvent.context.did.get, "");
            val noOfInteractEvents = DataFilter.filter(events, Filter("edata.type", "IN", Option(List("TOUCH", "DRAG", "DROP", "PINCH", "ZOOM", "SHAKE", "ROTATE", "SPEAK", "LISTEN", "WRITE", "DRAW", "START", "END", "CHOOSE", "ACTIVATE", "SCROLL")))).length;
            val interactEventsPerMin: Double = if (noOfInteractEvents == 0 || timeSpent == 0) 0d else BigDecimal(noOfInteractEvents / (timeSpent / 60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            var interactionEvents = eventsWithTs.filter(f => "INTERACT".equals(f._1.eid)).map(f => {
                (f._1.edata.`type`, 1, f._2)
            })

            val activitySummary = interactionEvents.groupBy(_._1).map { case (group: String, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2, a._3 + b._3) } }.map(f => ActivitySummary(f._1, f._2, CommonUtil.roundDouble(f._3, 2)));
            val eventSummary = events.groupBy { x => x.eid }.map(f => EventSummary(f._1, f._2.length));

            val interruptSummary = computeInterruptSummary(DataFilter.filter(events, Filter("eid", "EQ", Option("INTERRUPT"))));

            val impressionEvents = DataFilter.filter(events, Filter("eid", "EQ", Option("IMPRESSION")));
            val screenSummary = computeScreenSummary(firstEvent, lastEvent, impressionEvents, interruptSummary);

            val interruptTime = CommonUtil.roundDouble((timeDiff - timeSpent) + (if (interruptSummary.size > 0) interruptSummary.map(f => f._2).sum else 0d), 2);

            val pdata = CommonUtil.getAppDetails(firstEvent)
            val channel = x.channel
            
            val mode = if(null == firstEvent.edata.mode || firstEvent.edata.mode.isEmpty()) DEFAULT_MODE else firstEvent.edata.mode

            val did = firstEvent.context.did.get
            (LearnerProfileIndex(x.userId, pdata.id, channel), new SessionSummary(gameId, gameVersion, noOfAttempts, timeSpent, interruptTime, timeDiff, startTimestamp, endTimestamp, None, None, Option(loc), Option(itemResponses), DtRange(startTimestamp,
                endTimestamp), interactEventsPerMin, Option(activitySummary), None, Option(screenSummary), noOfInteractEvents,
                eventSummary, CommonUtil.getEventSyncTS(lastEvent), contentType, mimeType, did, mode, Option(CommonUtil.getETags(firstEvent)), telemetryVer, pdata));

        }.filter(f => (f._2.timeSpent >= 1)).cache(); // Skipping the events, if timeSpent is -ve
        JobLogger.log("'screenerSummary' joining with LearnerProfile table to get group_user value for each learner")
        //Joining with LearnerProfile table to add group info
        val groupInfoSummary = screenerSummary.map(f => LearnerProfileIndex(f._1.learner_id, f._1.app_id, f._1.channel)).distinct().joinWithCassandraTable[LearnerProfile](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE).map { x => (LearnerProfileIndex(x._1.learner_id, x._1.app_id, x._1.channel), (x._2.group_user, x._2.anonymous_user)); }
        val sessionSummary = screenerSummary.leftOuterJoin(groupInfoSummary)
        sessionSummary.map { x => SessionSummaryOutput(x._1.learner_id, x._1.app_id, x._1.channel, x._2._1, x._2._2) }
    }

    override def postProcess(data: RDD[SessionSummaryOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { userMap =>
            val game = userMap.ss;
            val booleanTuple = userMap.groupInfo.getOrElse((false, false))
            val mid = CommonUtil.getMessageId("ME_SESSION_SUMMARY", userMap.userId, "SESSION", game.dtRange, game.id, Option(userMap.appId), Option(userMap.channel));
            val measures = Map(
                "itemResponses" -> game.itemResponses,
                "start_time" -> game.start_time,
                "end_time" -> game.end_time,
                "syncDate" -> game.syncDate,
                "timeDiff" -> game.timeDiff,
                "timeSpent" -> game.timeSpent,
                "interruptTime" -> game.interruptTime,
                "comments" -> game.comments,
                "mode" -> game.mode,
                "fluency" -> game.fluency,
                "noOfAttempts" -> game.noOfAttempts,
                "noOfInteractEvents" -> game.noOfInteractEvents,
                "interactEventsPerMin" -> game.interactEventsPerMin,
                "activitySummary" -> game.activitySummary,
                "completionStatus" -> game.completionStatus,
                "screenSummary" -> game.screenSummary,
                "eventsSummary" -> game.eventsSummary,
                "telemetryVersion" -> game.telemetryVer,
                "contentType" -> game.contentType,
                "mimeType" -> game.mimeType);
            MeasuredEvent("ME_SESSION_SUMMARY", System.currentTimeMillis(), game.syncDate, meEventVersion, mid, userMap.userId, userMap.channel, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "LearnerSessionSummary").asInstanceOf[String])), None, "SESSION", game.dtRange),
                Dimensions(None, Option(game.did), Option(new GData(game.id, game.ver)), None, None, None, Option(game.pdata), game.loc, Option(booleanTuple._1), Option(booleanTuple._2)),
                MEEdata(measures), game.etags);
        }
    }
}