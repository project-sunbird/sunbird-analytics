package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.OtherStage
import org.ekstep.analytics.framework.util.CommonUtil
import java.io.File
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.OnboardStage
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.DataFetcher

class TestGenieFunnelModel extends SparkSpec(null) {

    ignore should "Generates Funnel Summary from S3" in {

        val queries = Option(Array(Query(Option("prod-data-store"), Option("raw/"), Option("2017-01-04"))));
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));

        //val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data.log");
        val events = GenieFunnelModel.execute(rdd, None).collect
        
        val cidCount = events.map { x =>  
            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val d = eksMap.get("downloadComplete").getOrElse(null)
            val count = if(null!=d) d.asInstanceOf[FunnelStageSummary].count.get else 0
            val cid =  eksMap.get("correlationID").get.asInstanceOf[String]
            (cid, count)
        }.filter{x => "09a6eb33-2573-4d0c-8a23-9586e3ef3ded".equals(x._1)}
        
        val totalCount = cidCount.map{x=> x._2}.sum
        println("Count: "+totalCount)
        //events.length should be(4)
    }

    it should "generates funnel summary, from a data having one funnel" in {
        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data1.log");
        val events = GenieFunnelModel.execute(rdd, None).collect

        events.length should be(1)
        val event = events.last

        event.eid should be("ME_GENIE_FUNNEL")

        event.dimensions.did.get should be("2e9d6b184f491540f9be4b800a4ab4a62ea8e592")
        event.dimensions.sid.get should be("0acc89ad-24dc-4c8b-b4a9-23116db5966f")
        event.dimensions.funnel.get should be("ContentSearch")
        event.dimensions.onboarding.get should be(false)

        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.get("correlationID").get should be("7124904c-b7be-4944-8a95-8c1cedebf712")
        val timeSpent = eksMap.get("timeSpent").get.asInstanceOf[Double]
        timeSpent should be(1114.23)

    }

    it should "generates funnel summary, from a data having multiple funnel" in {

        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data2.log");
        val events = GenieFunnelModel.execute(rdd, None).collect
        events.length should be(8)

        val onbs = events.filter { x => "GenieOnboarding".equals(x.dimensions.funnel.get) }
        val contentSearch = events.filter { x => "ContentSearch".equals(x.dimensions.funnel.get) }

        onbs.length should be(2)
        contentSearch.length should be(4)

        val onb = onbs.head

        onb.dimensions.onboarding.get should be(true)
        val eksMap = onb.edata.eks.asInstanceOf[Map[String, AnyRef]]
        OnboardStage.values.foreach { x =>
            eksMap.contains(x.toString()) should be(true)
        }

        eksMap.get("timeSpent").get.asInstanceOf[Double] should be(0.63)

    }

    it should "test the events from a data having one funnel" in {

        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data3.log");
        val events = GenieFunnelModel.execute(rdd, None).collect
        events.length should be(1)

        val onb = events.filter { x => "GenieOnboarding".equals(x.dimensions.funnel.get) }

        onb.length should be(1)

        val e1 = onb.last

        e1.dimensions.onboarding.get should be(true)

        val eksMap1 = e1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap1.get("timeSpent").get should be(4.47)

        eksMap1.contains("welcomeContent") should be(true)
        eksMap1.contains("addChild") should be(true)
        eksMap1.contains("gotoLibrary") should be(true)
        eksMap1.contains("firstLesson") should be(true)
    }

    it should "generates funnel summary, from a data having multiple funnel in multiple session and having onboarding funnel" in {
        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data4.log");
        val events = GenieFunnelModel.execute(rdd, None).collect
        events.length should be(6)

        val onbEvents = events.filter { x => x.dimensions.onboarding.get == true }

        onbEvents.length should be(events.length)

        onbEvents.head.dimensions.sid.get should not be (onbEvents.last.dimensions.sid.get)
    }

    it should "generates funnel summary, from a data having two recommendations funnel having misisng stages" in {
        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data5.log");
        val events = GenieFunnelModel.execute(rdd, None).collect
        events.length should be(1)

        val event = events.last
        event.dimensions.did.get should be("2e9d6b184f491540f9be4b800a4ab4a62ea8e592")

        event.dimensions.funnel.get should be("ContentRecommendation")
        event.dimensions.onboarding.get should be(false)

        val eventEksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val stages = OtherStage.values.map { x => (x.toString(), eventEksMap.get(x.toString()).get.asInstanceOf[FunnelStageSummary]) }.toMap

        stages.size should be(5)
        val invokedStages = stages.filter { x => x._2.stageInvoked.get == 1 }
        invokedStages.size should be(4)

        stages.contains("listContent") should be(true)
        val listContent = stages.get("listContent").get
        listContent.count.get should be(1)
        listContent.stageInvoked.get should be(1)

        stages.contains("selectContent") should be(true)
        val selectContent = stages.get("selectContent").get
        selectContent.stageInvoked.get should be(0)
        selectContent should be(FunnelStageSummary("selectContent"))

        stages.contains("downloadInitiated") should be(true)
        val downloadInitiated = stages.get("downloadInitiated").get
        downloadInitiated.count.get should be(1)
        downloadInitiated.stageInvoked.get should be(1)

        stages.contains("downloadComplete") should be(true)
        val downloadComplete = stages.get("downloadComplete").get
        downloadComplete.count.get should be(1)
        downloadComplete.stageInvoked.get should be(1)

        stages.contains("contentPlayed") should be(true)
        val contentPlayed = stages.get("contentPlayed").get
        contentPlayed.count.get should be(1)
        contentPlayed.stageInvoked.get should be(1)

        val stagesTimeSpent = eventEksMap.get("timeSpent").get.asInstanceOf[Double]
        stagesTimeSpent should be(41.98)

    }

    //    it should "test the funnel summary events for the input having all funnel" in {
    //        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data6.log");
    //        val events = GenieFunnelModel.execute(rdd, None).collect
    //
    //        val funnels = events.map { x => x.dimensions.funnel.get }.toList.distinct
    //        funnels.size should be(3)
    //        funnels.contains("GenieOnboarding") should be(true)
    //        funnels.contains("ContentSearch") should be(true)
    //        funnels.contains("ExploreContent") should be(false)
    //        funnels.contains("ContentRecommendation") should be(true)
    //    }

    it should "test the event for the input of GE_INTERACT events, but not having any funnel" in {
        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data7.log");
        val inputs = rdd.collect()
        inputs.length should be > (0)
        inputs.length should be(14)
        inputs.filter { x => "GE_INTERACT".equals(x.eid) }.length should be(inputs.length)

        val events = GenieFunnelModel.execute(rdd, None).collect
        events.length should be(0)
    }

}