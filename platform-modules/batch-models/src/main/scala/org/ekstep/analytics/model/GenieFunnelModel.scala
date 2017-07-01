package org.ekstep.analytics.model

import org.ekstep.analytics.util.SessionBatchModel
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.util.JSONUtils
import scala.collection.mutable.ListBuffer
import org.ekstep.analytics.framework.util.CommonUtil
import scala.collection.mutable.HashMap
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.OnboardStage
import org.ekstep.analytics.framework.OtherStage
import org.ekstep.analytics.framework.Stage
import org.ekstep.analytics.framework.CData
import org.ekstep.analytics.framework.conf.AppConf

case class GenieFunnelSession(channelId: String, did: String, cid: String, dspec: Map[String, AnyRef], funnel: String, events: Buffer[Event], onbFlag: Boolean) extends AlgoInput
case class GenieFunnel(funnel: String, cid: String, did: String, sid: String, dspec: Map[String, AnyRef], genieVer: String, summary: HashMap[String, FunnelStageSummary], timeSpent: Double, onboarding: Boolean, syncts: Long, dateRange: DtRange, tags: Option[AnyRef], appId: String, channelId: String) extends AlgoOutput

case class FunnelStageSummary(label: String, count: Option[Int] = Option(0), stageInvoked: Option[Int] = Option(0))

object GenieFunnelModel extends SessionBatchModel[Event, MeasuredEvent] with IBatchModelTemplate[Event, GenieFunnelSession, GenieFunnel, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.GenieFunnelModel"
    override def name: String = "GenieFunnelModel"

    def computeFunnelSummary(event: GenieFunnelSession): GenieFunnel = {

        var stageMap = HashMap[String, FunnelStageSummary]();

        val funnel = event.funnel
        val events = event.events

        var stageList = ListBuffer[(String, Double)]();
        if (events.length > 0) {
            var prevEvent = events(0);
            if ("GenieOnboarding".equals(funnel)) {
                _fillEmptySumm(stageMap, OnboardStage)
            } else {
                _fillEmptySumm(stageMap, OtherStage)
            }
            events.foreach { x =>
                x.eid match {
                    case "GE_INTERACT" =>
                        val stage = getStage(x.edata.eks.stageid, x.edata.eks.subtype)
                        if (stage.nonEmpty) {
                            stageList += Tuple2(stage, CommonUtil.roundDouble(CommonUtil.getTimeDiff(prevEvent, x).get, 2));
                        }

                    case "GE_LAUNCH_GAME" =>
                        stageList += Tuple2("contentPlayed", CommonUtil.roundDouble(CommonUtil.getTimeDiff(prevEvent, x).get, 2));
                }
                prevEvent = x;
            }

            stageList.foreach { x =>
                if (stageMap.contains(x._1)) {
                    stageMap.put(x._1, FunnelStageSummary(x._1, Option(stageMap.get(x._1).get.count.get + 1), Option(1)));
                } else {
                    stageMap.put(x._1, FunnelStageSummary(x._1, Option(1), Option(1)));
                }
            }
        }

        val firstEvent = events.head
        val endEvent = events.last

        val appId = firstEvent.appid.getOrElse(AppConf.getConfig("default.app.id"))
        val channelId = event.channelId

        val totalTimeSpent = CommonUtil.roundDouble(stageList.map { x => x._2 }.sum, 2)

        val dateRange = DtRange(CommonUtil.getEventTS(firstEvent), CommonUtil.getEventTS(endEvent))
        val syncts = CommonUtil.getEventSyncTS(endEvent)

        if ("GenieOnboarding".equals(funnel)) {
            stageMap = _updateSkippedStageSummary(stageMap);
        }
        GenieFunnel(funnel, event.cid, event.did, firstEvent.sid, event.dspec, firstEvent.gdata.ver, stageMap, totalTimeSpent, event.onbFlag, syncts, dateRange, Option(endEvent.tags), appId, channelId);
    }

    private def _updateSkippedStageSummary(stageMap: HashMap[String, FunnelStageSummary]): HashMap[String, FunnelStageSummary] = {
        stageMap.map { x =>
            (x._1, x._2.stageInvoked.get) match {
                case ("welcomeContent", 0) => (x._1, FunnelStageSummary(x._1, Option(1), Option(1)));
                case ("welcomeContent", 1) => (x._1, FunnelStageSummary(x._1, Option(0), Option(0)));

                case ("addChild", 0)       => (x._1, FunnelStageSummary(x._1, Option(1), Option(1)));
                case ("addChild", 1)       => (x._1, FunnelStageSummary(x._1, Option(0), Option(0)));

                case ("firstLesson", 0)    => (x._1, FunnelStageSummary(x._1, Option(1), Option(1)));
                case ("firstLesson", 1)    => (x._1, FunnelStageSummary(x._1, Option(0), Option(0)));

                case ("gotoLibrary", 0)    => (x._1, FunnelStageSummary(x._1, Option(1), Option(1)));
                case ("gotoLibrary", 1)    => (x._1, FunnelStageSummary(x._1, Option(0), Option(0)));

                case ("searchLesson", 0)   => (x._1, FunnelStageSummary(x._1, Option(1), Option(1)));
                case ("searchLesson", 1)   => (x._1, FunnelStageSummary(x._1, Option(0), Option(0)));
                case _                     => (x._1, x._2)
            }
        }
    }
    private def getStage(stageId: String, subType: String): String = {
        (stageId, subType) match {
            // for Genie Onboarding
            case ("Genie-Home-OnBoardingScreen", "WelcomeContent-Skipped") => "welcomeContent";
            case ("Genie-Home-OnBoardingScreen", "AddChild-Skipped") => "addChild";
            case ("Genie-Home-OnBoardingScreen", "FirstLesson-Skipped") => "firstLesson";
            case ("Genie-Home-OnBoardingScreen", "GoToLibrary-Skipped") => "gotoLibrary";
            case ("Genie-Home-OnBoardingScreen", "SearchLesson-Skipped") => "searchLesson";
            case ("Genie-Home-OnBoardingScreen", "") => "loadOnboardPage";
            // for others
            case ("ContentSearch", "SearchPhrase") => "listContent";
            case ("ContentList", "SearchPhrase") => "listContent";
            case ("ContentList", "ContentClicked") => "selectContent";
            case ("ContentDetail", "ContentDownload-Initiate") => "downloadInitiated";
            case ("ContentDetail", "ContentDownload-Success") => "downloadComplete";
            case ("ExploreContent", "ContentClicked") => "selectContent";
            case ("ExploreContent", "") => "listContent";
            case _ => "";
        }
    }

    private def _fillEmptySumm(stageMap: HashMap[String, FunnelStageSummary], stage: Stage) {
        stage.values.foreach { x =>
            stageMap.put(x.toString(), FunnelStageSummary(x.toString()));
        }
    }

    private def _getFunnelId(cdata: CData, stageId: String, subType: String): String = {
        val cdataType = cdata.`type`.get
        //if ("ONBRDNG".equals(cdataType)) "GenieOnboarding"; else if ("org.ekstep.recommendation".equals(cdataType)) "ContentRecommendation"; else if (("ContentSearch".equals(stageId) || "ContentList".equals(stageId)) && "SearchPhrase".equals(subType)) "ContentSearch"; else if ("ExploreContent".equals(stageId) && ("".equals(subType) || "ContentClicked".equals(subType))) "ExploreContent"; else "";
        //if ("ONBRDNG".equals(cdataType)) "GenieOnboarding"; else if ("org.ekstep.recommendation".equals(cdataType)) "ContentRecommendation"; else if ("ContentSearch".equals(stageId) && "SearchPhrase".equals(subType)) "ContentSearch"; else if ("ContentList".equals(stageId) && "SearchPhrase".equals(subType)) "ExploreContent"; else if ("ExploreContent".equals(stageId) && ("".equals(subType) || "ContentClicked".equals(subType))) "ExploreContent"; else "";
        if ("ONBRDNG".equalsIgnoreCase(cdataType)) {
            "GenieOnboarding";
        } else if ("org.ekstep.recommendation".equals(cdataType) || "api-ekstep.analytics.recommendations".equals(cdataType)) {
            "ContentRecommendation";
        } else if ("ContentSearch".equals(stageId) && "SearchPhrase".equals(subType)) {
            "ContentSearch";
        } else if ("ContentList".equals(stageId) || "ExploreContent".equals(stageId) || "ContentClicked".equals(subType)) {
            "ExploreContent";
        } else "";
    }

    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GenieFunnelSession] = {
        val idleTime = config.getOrElse("idleTime", 30).asInstanceOf[Int]
        val genieLaunchSessions = getGenieLaunchSessions(data, idleTime);

        genieLaunchSessions.mapValues { x =>
            val geStartEvents = DataFilter.filter(x, Filter("eid", "EQ", Option("GE_GENIE_START")))
            val dspec = if (geStartEvents.size > 0) {
                if (null != geStartEvents.last.edata)
                    geStartEvents.last.edata.eks.dspec;
                else null;
            } else null;

            val filteredData = DataFilter.filter(x, Filter("eid", "IN", Option(List("GE_LAUNCH_GAME", "GE_INTERACT")))).filter { x => x.cdata != null && x.cdata.nonEmpty }
            val onb = filteredData.filter { x => "Genie-Home-OnBoardingScreen".equals(x.edata.eks.stageid) }
            val onbflag = if (onb.length > 0) true; else false;
            filteredData.flatMap { x => x.cdata.filter { x => null != x.id }.map { y => (y.id, y, x) } }.groupBy { x => x._1 }.map { x => (x._1, dspec, x._2.map(y => y._3), x._2.map(y => y._2).head, onbflag) };
        }.map { x =>
            val did = x._1._2
            val channelId = x._1._1
            x._2.map { x =>
                val events = x._3.sortBy { x => CommonUtil.getEventTS(x) }
                val firstEvent = events.head
                val funnel = _getFunnelId(x._4, firstEvent.edata.eks.stageid, firstEvent.edata.eks.subtype)
                GenieFunnelSession(channelId, did, x._1, x._2, funnel, events, x._5)
            };
        }.flatMap { x => x }.filter { x => !"".equals(x.funnel) };

    }

    override def algorithm(data: RDD[GenieFunnelSession], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GenieFunnel] = {
        data.map { x =>
            computeFunnelSummary(x)
        }
    }

    override def postProcess(data: RDD[GenieFunnel], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.filter { x => x.timeSpent > 0 }.map { summary =>
            val mid = CommonUtil.getMessageId("ME_GENIE_FUNNEL", summary.funnel + summary.cid, config.getOrElse("granularity", "FUNNEL").asInstanceOf[String], summary.dateRange, summary.did);
            val measures = summary.summary.toMap ++ Map("timeSpent" -> summary.timeSpent, "correlationID" -> summary.cid)
            MeasuredEvent("ME_GENIE_FUNNEL", System.currentTimeMillis(), summary.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieFunnel").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "FUNNEL").asInstanceOf[String], summary.dateRange),
                Dimensions(None, Option(summary.did), None, None, None, None, None, None, None, None, None, None, None, None, Option(summary.sid), None, Option(summary.funnel), Option(summary.dspec), Option(summary.onboarding), Option(summary.genieVer), None, None, None, Option(summary.appId), None, None, Option(summary.channelId)),
                MEEdata(measures), summary.tags);
        }
    }

}