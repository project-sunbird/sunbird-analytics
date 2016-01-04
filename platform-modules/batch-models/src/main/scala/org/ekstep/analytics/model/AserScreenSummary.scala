package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
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
import org.ekstep.analytics.framework.SessionBatchModel

case class AserScreener(var activationKeyPage: Option[Double] = Option(0d), var surveyCodePage: Option[Double] = Option(0d),
                        var childReg1: Option[Double] = Option(0d), var childReg2: Option[Double] = Option(0d), var childReg3: Option[Double] = Option(0d),
                        var assessLanguage: Option[Double] = Option(0d), var languageLevel: Option[Double] = Option(0d), var selectNumeracyQ1: Option[Double] = Option(0d),
                        var assessNumeracyQ1: Option[Double] = Option(0d), var selectNumeracyQ2: Option[Double] = Option(0d),
                        var assessNumeracyQ2: Option[Double] = Option(0d), var assessNumeracyQ3: Option[Double] = Option(0d),
                        var scorecard: Option[Double] = Option(0d), var summary: Option[Double] = Option(0d))
/**
 * Aser Screen Summary Model
 */
class AserScreenSummary extends SessionBatchModel with Serializable {

    def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);
        val gameSessions = getGameSessions(events);
        val aserSreenSummary = gameSessions.mapValues { x =>
            
            val startTimestamp = if (x.length > 0) { Option(CommonUtil.getEventTS(x(0))) } else { Option(0l) };
            val endTimestamp = if (x.length > 0) { Option(CommonUtil.getEventTS(x.last)) } else { Option(0l) };
            val oeStart: Event = x(0);

            var storyReading: Event = null;
            var q1Select: Event = null;
            var q2Select: Event = null;
            var endTest: Event = null;
            var endMath: Event = null;
            var exit: Event = null;

            var oeInteractStartButton: Event = null;
            val oeInteractNextButton = x.filter(e => "OE_INTERACT".equals(e.eid)).filter { e => "Next button pressed".equals(e.edata.eks.id) };
            val oeAssess = x.filter(e => "OE_ASSESS".equals(e.eid));

            x.filter(e => "OE_INTERACT".equals(e.eid)).foreach { e =>
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
            (as, DtRange(startTimestamp.getOrElse(0l), endTimestamp.getOrElse(0l)), CommonUtil.getGameId(x(0)), CommonUtil.getGameVersion(x(0)));
        }

        aserSreenSummary.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }
    /**
     * Get the measured event from the UserMap
     */
    private def getMeasuredEvent(userMap: (String, (AserScreener, DtRange,String,String)), config: Map[String, AnyRef]): MeasuredEvent = {
        val measures = userMap._2._1;
        MeasuredEvent(config.getOrElse("eventId", "ME_ASER_SCREENER_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "AserScreenerSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "SESSION", userMap._2._2),
            Dimensions(None, Option(new GData(userMap._2._3, userMap._2._4)), None, None, None, None),
            MEEdata(measures));
    }
}