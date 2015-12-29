package org.ekstep.analytics.model

import org.ekstep.ilimi.analytics.framework.IBatchModel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.Event
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import scala.collection.mutable.Buffer
import org.ekstep.ilimi.analytics.framework.JobContext
import org.apache.spark.HashPartitioner
import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue
import java.util.ArrayList
import org.ekstep.ilimi.analytics.framework.MeasuredEvent
import org.ekstep.ilimi.analytics.framework.Context
import org.ekstep.ilimi.analytics.framework.PData
import org.ekstep.ilimi.analytics.framework.Dimensions
import org.ekstep.ilimi.analytics.framework.GData
import org.ekstep.ilimi.analytics.framework.MEEdata
import org.ekstep.ilimi.analytics.framework.util.JSONUtils
import org.json4s.JsonUtil
import java.io.FileWriter

case class AserScreener(var activationKeyPage: Option[Double] = Option(0d), var surveyCodePage: Option[Double] = Option(0d), var childReg1: Option[Double] = Option(0d), var childReg2: Option[Double] = Option(0d), var childReg3: Option[Double] = Option(0d), var assessLanguage: Option[Double] = Option(0d), var languageLevel: Option[Double] = Option(0d), var selectNumeracyQ1: Option[Double] = Option(0d), var assessNumeracyQ1: Option[Double] = Option(0d), var selectNumeracyQ2: Option[Double] = Option(0d), var assessNumeracyQ2: Option[Double] = Option(0d), var assessNumeracyQ3: Option[Double] = Option(0d), var scorecard: Option[Double] = Option(0d), var summary: Option[Double] = Option(0d))
/**
 * Aser Screen Summary Model
 */
class AserScreenSummary extends IBatchModel with Serializable {

    def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val gameSessions = events.filter { x => x.uid != null }
            .map(event => (event.uid, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
                var sessions = Buffer[Buffer[Event]]();
                var tmpArr = Buffer[Event]();

                x.foreach { y =>
                    y.eid match {
                        case "OE_START" =>
                            if (tmpArr.length > 0) {
                                sessions += tmpArr;
                                tmpArr = Buffer[Event]();
                            }
                            tmpArr += y;
                        case _ =>
                            ;
                            tmpArr += y;
                    }
                }
                sessions += tmpArr;
                sessions;
            }.flatMap(f => f._2.map { x => (f._1, x) });

        val aserSreenSummary = gameSessions.mapValues { x =>
            
            var oeStart: Event = null;
            var oeInteractStartButton: Event = null;
            var storyReading: Event = null;
            var q1Select: Event = null;
            var q2Select: Event = null;
            var endTest: Event = null;
            var endMath: Event = null;
            var exit: Event = null;

            var oeInteractNextButton = Buffer[Event]();
            var oeAssess = Buffer[Event]();

            x.foreach { y =>
                y.eid match {
                    case "OE_START" =>
                        oeStart = y;
                    case "OE_INTERACT" =>
                        var id = y.edata.eks.id
                        if (id.equals("Next button pressed")) {
                            oeInteractNextButton += y;
                        } else if (id.equals("Start button pressed")) {
                            oeInteractStartButton = y;
                        } else if (id.contains("read story radio button selected")) {
                            storyReading = y;
                        } else if (id.equals("Question one selected")) {
                            q1Select = y;
                        } else if (id.equals("Question two selected")) {
                            q2Select = y;
                        } else if (id.equals("End test button pressed")) {
                            endTest = y;
                        } else if (id.equals("End math test button pressed")) {
                            endMath = y;
                        } else if (id.equals("Exit button pressed")) {
                            exit = y;
                        }
                    case "OE_ASSESS" =>
                        oeAssess += y;
                    case _ => ;
                }
            }
            var as = AserScreener();

            // Initializing 1st 5 Registration pages
            if (oeInteractNextButton.length > 0)
                as.activationKeyPage = CommonUtil.getTimeDiff(oeStart, oeInteractNextButton(0));
            if (oeInteractNextButton.length > 1) 
                as.surveyCodePage = CommonUtil.getTimeDiff(oeStart, oeInteractNextButton(1));
            if (oeInteractNextButton.length > 2)
                as.childReg1 = CommonUtil.getTimeDiff(oeStart, oeInteractNextButton(2));
            if (oeInteractNextButton.length > 3)
                as.childReg2 = CommonUtil.getTimeDiff(oeStart, oeInteractNextButton(3));
            if (oeInteractNextButton.length > 4) 
                as.childReg3 = CommonUtil.getTimeDiff(oeStart, oeInteractNextButton(4));
            //-----------
            if (oeAssess.size > 2) {
                var first: Event = oeAssess(0)
                var sec: Event = oeAssess(1)
                var third: Event = oeAssess(2)

                var firstOeAssLen = CommonUtil.getTimeSpent(first.edata.eks.length)
                var secOeAssLen = CommonUtil.getTimeSpent(sec.edata.eks.length)
                var thirdOeAssLen = CommonUtil.getTimeSpent(third.edata.eks.length)

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

                //assess Q1
                if (secOeAssLen.get != 0)
                    as.assessNumeracyQ1 = secOeAssLen;
                else if (q1Select != null)
                    as.assessNumeracyQ1 = CommonUtil.getTimeDiff(q1Select, sec);

                // select Q2
                if (q2Select != null)
                    as.selectNumeracyQ2 = CommonUtil.getTimeDiff(sec, q2Select);

                // assess Q2
                if (thirdOeAssLen.get != 0)
                    as.assessNumeracyQ2 = thirdOeAssLen;
                else if (q2Select != null)
                    as.assessNumeracyQ2 = CommonUtil.getTimeDiff(q2Select, third);

                if (oeAssess.size > 3) {
                    var fourth = oeAssess(3)
                    var fourthOeAssLen = CommonUtil.getTimeSpent(fourth.edata.eks.length)
                    // assess Q3
                    if (fourthOeAssLen.get != 0)
                        as.assessNumeracyQ3 = fourthOeAssLen;
                    else
                        as.assessNumeracyQ3 = CommonUtil.getTimeDiff(third, fourth);

                    if (endMath != null && endTest != null)
                        as.scorecard = CommonUtil.getTimeDiff(endMath, endTest);
                    if (endTest != null && exit != null)
                        as.summary = CommonUtil.getTimeDiff(endTest, exit);

                }
            } else if (oeAssess.size == 2) { // when language test and only one question are attended 
                var first: Event = oeAssess(0)
                var sec: Event = oeAssess(1)

                var firstOeAssLen = CommonUtil.getTimeSpent(first.edata.eks.length)
                var secOeAssLen = CommonUtil.getTimeSpent(sec.edata.eks.length)

                // language pages
                if (firstOeAssLen.get != 0)
                    as.assessLanguage = firstOeAssLen;
                else
                    as.assessLanguage = CommonUtil.getTimeDiff(oeInteractStartButton, first);

                if (storyReading != null)
                    as.languageLevel = CommonUtil.getTimeDiff(first, storyReading);

                //select Q1
                if (storyReading != null && q1Select != null)
                    as.selectNumeracyQ1 = CommonUtil.getTimeDiff(storyReading, q1Select);

                //assess Q1
                if (secOeAssLen.get != 0)
                    as.assessNumeracyQ1 = secOeAssLen;
                else if (q1Select != null)
                    as.assessNumeracyQ1 = CommonUtil.getTimeDiff(q1Select, sec);

                // score card & summary
                if (endMath != null && endTest != null)
                    as.scorecard = CommonUtil.getTimeDiff(endMath, endTest);
                if (endTest != null && exit != null)
                    as.summary = CommonUtil.getTimeDiff(endTest, exit);
            }
            as;
        }

        aserSreenSummary.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }
    /**
     * Get the measured event from the UserMap
     */
    private def getMeasuredEvent(userMap: (String, AserScreener), config: Map[String, AnyRef]): MeasuredEvent = {
        val measures = userMap._2;
        MeasuredEvent(config.getOrElse("eventId", "ASER_SCREENER_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "AserScreenerSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, Option("SESSION"), None),
            Dimensions(None, None, None, None, None, None),
            MEEdata(measures));
    }
}