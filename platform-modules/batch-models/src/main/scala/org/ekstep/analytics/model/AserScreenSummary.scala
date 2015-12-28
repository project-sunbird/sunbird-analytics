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

/**
 * Case class to hold the screener summary
 */

case class AserScreener(uid: String, pageSummary:Map[String,Double])

/**
 * Aser Screen Summary Model
 */
class AserScreenSummary extends IBatchModel with Serializable{
  
  val regPages = Array("activationKeyPage", "surveyCodePage", "childReg1","childReg2","childReg3")
  
  def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]) : RDD[String] =
  {
    
    val config = jobParams.getOrElse(Map[String, AnyRef]());
    val configMapping = sc.broadcast(config);
    
    val aserEvents = events.filter { x => x.uid.nonEmpty }
      .filter { x => x.eid.startsWith("OE") }
      .sortBy(x => x.ts)
      .map(event => (event.uid, Buffer(event)))
      .partitionBy(new HashPartitioner(JobContext.parallelization))
      .reduceByKey((a, b) => a ++ b);
      
    val aserSreenSummary = aserEvents.mapValues { x => 
    
      var pageSummary = HashMap[String,Option[Double]]();
      
      var oeStart:Event = null;
      var oeInteractStartButton:Event = null;
      var storyReading:Event = null;
      var q1Select:Event = null;
      var q2Select:Event = null;
      var endTest:Event = null;
      var endMath:Event = null;
      var exit:Event = null;
      
      var oeInteractNextButton = Buffer[Event]();
      var oeAssess = Buffer[Event]();
      
      x.foreach { y => 
        y.eid match {
          case "OE_START" => 
              oeStart = y;
          case "OE_INTERACT" =>
              var id = y.edata.eks.id
              if(id.equals("Next button pressed"))
              {
                oeInteractNextButton += y;
              }
              else if(id.equals("Start button pressed"))
              {
                 oeInteractStartButton = y;
              }
              else if(id.contains("read story radio button selected"))
              {
                 storyReading = y;
              }
              else if(id.equals("Question one selected"))
              {
                 q1Select = y;
              }
              else if(id.equals("Question one selected"))
              {
                 q2Select = y;
              }
              else if(id.equals("End test button pressed"))
              {
                 endTest = y;
              }
              else if(id.equals("End math test button pressed"))
              {
                 endMath = y;
              }
              else if(id.equals("Exit button pressed"))
              {
                 exit = y;
              }
          case "OE_ASSESS" =>
              oeAssess += y;
          case _ => ;  
        }
      }
      
      // Initializing 1st 5 Registration pages
      if(oeInteractNextButton.size>0)
      {
        pageSummary("activationKeyPage") = CommonUtil.getTimeDiff(oeStart, oeInteractNextButton(0))
        
        for((x,i) <- oeInteractNextButton.view.zipWithIndex)
        {
          if(i>0&&i<5)
          {
             pageSummary(regPages.apply(i)) = CommonUtil.getTimeDiff(oeInteractNextButton(i-1), x) 
          }
        }
        //Initializing with default if not available 
        for( i <- oeInteractNextButton.size to regPages.size-1){
         pageSummary(regPages.apply(i)) = Option(0.0d);
        }
      }
      else
      {
        for( i <- 0 to regPages.size-1){
         pageSummary(regPages.apply(i)) = Option(0.0d);
        }
      }
      
      if(oeAssess.size>2)
      {
        var first:Event = oeAssess(0)
        var sec:Event = oeAssess(1)
        var third:Event = oeAssess(2)
        
        var firstOeAssLen = CommonUtil.getTimeSpent(first.edata.eks.length)
        var secOeAssLen = CommonUtil.getTimeSpent(sec.edata.eks.length)
        var thirdOeAssLen = CommonUtil.getTimeSpent(third.edata.eks.length)
        
        if(firstOeAssLen.get!=0){
          pageSummary("assessLanguage") = firstOeAssLen
        }
        else 
        {
           if(oeInteractStartButton!=null)
             pageSummary("assessLanguage") = CommonUtil.getTimeDiff(oeInteractStartButton, first)
           else
             pageSummary("assessLanguage") = Option(0.0d);
        }
        
        if(storyReading!=null)
          pageSummary("languageLevel") = CommonUtil.getTimeDiff(first,storyReading);
        else
          pageSummary("languageLevel") = Option(0.0d);
        
        if(storyReading!=null&&q1Select!=null)
          pageSummary("selectNumeracyQ1") = CommonUtil.getTimeDiff(storyReading,q1Select);
        else
          pageSummary("selectNumeracyQ1") = Option(0.0d);
        
        //Q1
        if(secOeAssLen.get!=0)
        {
          pageSummary("assessNumeracyQ1") = secOeAssLen
        }
        else 
        {
           if(q1Select!=null)
             pageSummary("assessNumeracyQ1") = CommonUtil.getTimeDiff(q1Select, sec);
           else
             pageSummary("assessNumeracyQ1") = Option(0.0d);
             
        }
        
        if(q2Select!=null)
          pageSummary("selectNumeracyQ2") = CommonUtil.getTimeDiff(sec,q2Select);
        else
          pageSummary("selectNumeracyQ2") = Option(0.0d);
        
        
        // Q2
        if(thirdOeAssLen.get!=0)
        {
          pageSummary("assessNumeracyQ2") = thirdOeAssLen;
        }
        else 
        {
          if(q2Select!=null)
            pageSummary("assessNumeracyQ2") = CommonUtil.getTimeDiff(q2Select, third);
          else
            pageSummary("assessNumeracyQ2") = Option(0.0d);
        }
        
        if(oeAssess.size>3)
        {
          var fourth = oeAssess(3)
          var fourthOeAssLen = CommonUtil.getTimeSpent(fourth.edata.eks.length)
          // Q3
          if(fourthOeAssLen.get!=0)
          {
            pageSummary("assessNumeracyQ3") = fourthOeAssLen
          }
          else 
          {
             pageSummary("assessNumeracyQ3") = CommonUtil.getTimeDiff(third,fourth) 
          }
        }
        else
        {
          pageSummary("assessNumeracyQ3") = Option(0.0d)
        }
        
        if(endMath!=null&&endTest!=null)
          pageSummary("scorecard") = CommonUtil.getTimeDiff(endMath,endTest);
        else
          pageSummary("scorecard") = Option(0.0d);
        
        if(endTest!=null&&exit!=null)
          pageSummary("summary") = CommonUtil.getTimeDiff(endTest,exit);
        else
          pageSummary("summary") = Option(0.0d)
        
      }
      else if(oeAssess.size==2)
      {
        var first:Event = oeAssess(0)
        var sec:Event = oeAssess(1)
        
        var firstOeAssLen = CommonUtil.getTimeSpent(first.edata.eks.length)
        var secOeAssLen = CommonUtil.getTimeSpent(sec.edata.eks.length)
        
        if(firstOeAssLen.get!=0){
          pageSummary("assessLanguage") = firstOeAssLen
        }
        else 
        {
           pageSummary("assessLanguage") = CommonUtil.getTimeDiff(oeInteractStartButton, first) 
        }
        
        if(storyReading!=null)
          pageSummary("languageLevel") = CommonUtil.getTimeDiff(first,storyReading);
        else
          pageSummary("languageLevel") = Option(0.0d);
        
        if(storyReading!=null&&q1Select!=null)
          pageSummary("selectNumeracyQ1") = CommonUtil.getTimeDiff(storyReading,q1Select);
        else
          pageSummary("selectNumeracyQ1") = Option(0.0d);
        
        //Q1
        if(secOeAssLen.get!=0)
        {
          pageSummary("assessNumeracyQ1") = secOeAssLen
        }
        else 
        {
           
           if(q1Select!=null)
             pageSummary("assessNumeracyQ1") = CommonUtil.getTimeDiff(q1Select, sec);
           else
             pageSummary("assessNumeracyQ1") = Option(0.0d);
        }
        
        pageSummary("selectNumeracyQ2") = Option(0.0d)
        pageSummary("assessNumeracyQ2") = Option(0.0d)
        pageSummary("assessNumeracyQ3") = Option(0.0d)
        
        if(endMath!=null&&endTest!=null)
          pageSummary("scorecard") = CommonUtil.getTimeDiff(endMath,endTest);
        else
          pageSummary("scorecard") = Option(0.0d);
        
        if(endTest!=null&&exit!=null)
          pageSummary("summary") = CommonUtil.getTimeDiff(endTest,exit);
        else
          pageSummary("summary") = Option(0.0d)
        
      }
      pageSummary;
    } 
        
     aserSreenSummary.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
  }
  /**
     * Get the measured event from the UserMap
     */
    private def getMeasuredEvent(userMap: (String, HashMap[String,Option[Double]]), config: Map[String, AnyRef]): MeasuredEvent = {
        val measures = userMap._2;
        
        MeasuredEvent(config.getOrElse("eventId", "ASER_SCREENER_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None, 
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "AserScreenerSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, Option("SESSION"), None), 
                Dimensions(None, None, None, None, None, None), 
                MEEdata(measures));
    }
}