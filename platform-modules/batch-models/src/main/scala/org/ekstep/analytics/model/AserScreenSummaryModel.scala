package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.CommonUtil
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.JobContext
import org.apache.spark.HashPartitioner
import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue
import java.util.ArrayList
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.util.JSONUtils
import org.json4s.JsonUtil
import java.io.FileWriter
import org.ekstep.analytics.framework.DtRange
import java.security.MessageDigest
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.ekstep.analytics.util.SessionBatchModel

case class AserScreener(var activationKeyPage: Option[Double] = Option(0d), var surveyCodePage: Option[Double] = Option(0d),
                        var childReg1: Option[Double] = Option(0d), var childReg2: Option[Double] = Option(0d), var childReg3: Option[Double] = Option(0d),
                        var assessLanguage: Option[Double] = Option(0d), var languageLevel: Option[Double] = Option(0d), var selectNumeracyQ1: Option[Double] = Option(0d),
                        var assessNumeracyQ1: Option[Double] = Option(0d), var selectNumeracyQ2: Option[Double] = Option(0d),
                        var assessNumeracyQ2: Option[Double] = Option(0d), var assessNumeracyQ3: Option[Double] = Option(0d),
                        var scorecard: Option[Double] = Option(0d), var summary: Option[Double] = Option(0d),
                        var userId: String = "", var dtRange: DtRange = DtRange(0l, 0l), var gameId: String = "",
                        var gameVersion: String = "", var did: String = "", var timeStamp: Long = 0l) extends AlgoOutput

case class AserScreenerInput(channelId: String, userId: String, gameSessions: Buffer[Event]) extends AlgoInput

/**
 * Aser Screen Summary Model
 */
object AserScreenSummaryModel extends SessionBatchModel[Event, MeasuredEvent] with IBatchModelTemplate[Event, AserScreenerInput, AserScreener, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.AserScreenSummaryModel"
    override def name(): String = "AserScreenSummaryModel"

    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AserScreenerInput] = {
        val configMapping = sc.broadcast(config);
        val gameSessions = getGameSessions(data.filter { x => "org.ekstep.aser.lite".equals(x.gdata.id) });
        gameSessions.map { x => AserScreenerInput(x._1._1, x._1._2, x._2) }
    }

    override def algorithm(data: RDD[AserScreenerInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AserScreener] = {

        data.map { x =>

            val startTimestamp = Option(CommonUtil.getEventTS(x.gameSessions(0)));
            val endTimestamp = Option(CommonUtil.getEventTS(x.gameSessions.last));

            val oeStart: Event = x.gameSessions(0);
            val did = oeStart.did
            var storyReading: Event = null;
            var q1Select: Event = null;
            var q2Select: Event = null;
            var endTest: Event = null;
            var endMath: Event = null;
            var exit: Event = null;

            var oeInteractStartButton: Event = null;
            val oeInteractNextButton = x.gameSessions.filter(e => "OE_INTERACT".equals(e.eid)).filter { e => "Next button pressed".equals(e.edata.eks.id) };
            val oeAssess = x.gameSessions.filter(e => "OE_ASSESS".equals(e.eid));

            x.gameSessions.filter(e => "OE_INTERACT".equals(e.eid)).foreach { e =>
                e.edata.eks.id.toLowerCase() match {
                    case "start button pressed"                    => oeInteractStartButton = e;
                    case "can read story radio button selected"    => storyReading = e;
                    case "cannot read story radio button selected" => storyReading = e;
                    case "question one selected"                   => q1Select = e;
                    case "question two selected"                   => q2Select = e;
                    case "end test button pressed"                 => endTest = e;
                    case "end math test button pressed"            => endMath = e;
                    case "exit button pressed"                     => exit = e;
                    case _                                         =>
                }

            }
            var as = AserScreener();

            // Initializing 1st 5 Registration pages
            if (oeInteractNextButton.length > 0 && oeStart != null)
                as.activationKeyPage = CommonUtil.getTimeDiff(oeStart, oeInteractNextButton(0));
            if (oeInteractNextButton.length > 1)
                as.surveyCodePage = CommonUtil.getTimeDiff(oeInteractNextButton(0), oeInteractNextButton(1));
            if (oeInteractNextButton.length > 2)
                as.childReg1 = CommonUtil.getTimeDiff(oeInteractNextButton(1), oeInteractNextButton(2));
            if (oeInteractNextButton.length > 3)
                as.childReg2 = CommonUtil.getTimeDiff(oeInteractNextButton(2), oeInteractNextButton(3));
            if (oeInteractNextButton.length > 4)
                as.childReg3 = CommonUtil.getTimeDiff(oeInteractNextButton(3), oeInteractNextButton(4));
            //-----------
            if (oeAssess.size > 0) {
                var first: Event = oeAssess(0)
                var firstOeAssLen = CommonUtil.getTimeSpent(first.edata.eks.length)

                //language
                if (firstOeAssLen.get != 0)
                    as.assessLanguage = firstOeAssLen;
                else if (oeInteractStartButton != null)
                    as.assessLanguage = CommonUtil.getTimeDiff(oeInteractStartButton, first);
                if (storyReading != null)
                    as.languageLevel = CommonUtil.getTimeDiff(first, storyReading);

                //select Q1
                if (storyReading != null && q1Select != null)
                    as.selectNumeracyQ1 = CommonUtil.getTimeDiff(storyReading, q1Select);
            }
            if (oeAssess.size > 1) {
                var sec: Event = oeAssess(1)
                var secOeAssLen = CommonUtil.getTimeSpent(sec.edata.eks.length)

                //assess Q1
                if (secOeAssLen.get != 0)
                    as.assessNumeracyQ1 = secOeAssLen;
                else if (q1Select != null)
                    as.assessNumeracyQ1 = CommonUtil.getTimeDiff(q1Select, sec);

                // select Q2
                if (q2Select != null)
                    as.selectNumeracyQ2 = CommonUtil.getTimeDiff(sec, q2Select);
            }
            var third: Event = null
            if (oeAssess.size > 2) {
                third = oeAssess(2)
                var thirdOeAssLen = CommonUtil.getTimeSpent(third.edata.eks.length)

                // assess Q2
                if (thirdOeAssLen.get != 0)
                    as.assessNumeracyQ2 = thirdOeAssLen;
                else if (q2Select != null)
                    as.assessNumeracyQ2 = CommonUtil.getTimeDiff(q2Select, third);
            }
            if (oeAssess.size > 3) {
                var fourth = oeAssess(3)
                var fourthOeAssLen = CommonUtil.getTimeSpent(fourth.edata.eks.length)

                // assess Q3
                if (fourthOeAssLen.get != 0)
                    as.assessNumeracyQ3 = fourthOeAssLen;
                else
                    as.assessNumeracyQ3 = CommonUtil.getTimeDiff(third, fourth);
            }
            // score card & summary
            if (endMath != null && endTest != null)
                as.scorecard = CommonUtil.getTimeDiff(endMath, endTest);
            if (endTest != null && exit != null)
                as.summary = CommonUtil.getTimeDiff(endTest, exit);

            as.userId = x.userId;
            as.dtRange = DtRange(startTimestamp.getOrElse(0l), endTimestamp.getOrElse(0l))
            as.gameId = CommonUtil.getGameId(x.gameSessions(0))
            as.gameVersion = CommonUtil.getGameVersion(x.gameSessions(0))
            as.did = did;
            as.timeStamp = CommonUtil.getTimestamp(oeStart.`@timestamp`)
            as
        }
    }

    override def postProcess(data: RDD[AserScreener], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { aserScreener =>
            val measures = Map(
                "activationKeyPage" -> aserScreener.activationKeyPage.get,
                "surveyCodePage" -> aserScreener.surveyCodePage.get,
                "childReg1" -> aserScreener.childReg1.get,
                "childReg2" -> aserScreener.childReg2.get,
                "childReg3" -> aserScreener.childReg3.get,
                "assessLanguage" -> aserScreener.assessLanguage.get,
                "languageLevel" -> aserScreener.languageLevel.get,
                "selectNumeracyQ1" -> aserScreener.selectNumeracyQ1.get,
                "assessNumeracyQ1" -> aserScreener.assessNumeracyQ1.get,
                "selectNumeracyQ2" -> aserScreener.selectNumeracyQ2.get,
                "assessNumeracyQ2" -> aserScreener.assessNumeracyQ2.get,
                "assessNumeracyQ3" -> aserScreener.assessNumeracyQ3.get,
                "scorecard" -> aserScreener.scorecard.get,
                "summary" -> aserScreener.summary.get);
            val mid = CommonUtil.getMessageId("ME_ASER_SCREEN_SUMMARY", aserScreener.userId, "SESSION", aserScreener.dtRange, aserScreener.gameId);
            MeasuredEvent("ME_ASER_SCREEN_SUMMARY", System.currentTimeMillis(), aserScreener.timeStamp, mid, "1.0", aserScreener.userId, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "AserScreenerSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "SESSION", aserScreener.dtRange),
                Dimensions(None, Option(aserScreener.did), Option(new GData(aserScreener.gameId, aserScreener.gameVersion)), None, None, None, None),
                MEEdata(measures));
        }
    }
}