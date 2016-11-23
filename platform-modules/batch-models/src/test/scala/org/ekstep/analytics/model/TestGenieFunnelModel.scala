package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils

class TestGenieFunnelModel extends SparkSpec(null) {

    "GenieFunnelModel" should "Generates Funnel Summary" in {

        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data.log");
        val events = GenieFunnelModel.execute(rdd, None).collect
        events.length should be(35)
    }

    it should "generates funnel summary, from a data having one funnel in one session" in {
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
        val timeSpent = eksMap.get("timeSpent").get.asInstanceOf[Double]
        timeSpent should be(1114.23)

        val listContent = eksMap.get("listContent").get.asInstanceOf[FunnelStageSummary]
        val ts1 = listContent.timeSpent.get

        val selectContent = eksMap.get("selectContent").get.asInstanceOf[FunnelStageSummary]
        val ts2 = selectContent.timeSpent.get

        val downloadInitiated = eksMap.get("downloadInitiated").get.asInstanceOf[FunnelStageSummary]
        val ts3 = downloadInitiated.timeSpent.get

        val downloadComplete = eksMap.get("downloadComplete").get.asInstanceOf[FunnelStageSummary]
        val ts4 = downloadComplete.timeSpent.get

        (ts1 + ts2 + ts3 + ts4) should be(timeSpent)

    }

    it should "generates funnel summary, from a data having multiple funnel in one session" in {

        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data2.log");
        val events = GenieFunnelModel.execute(rdd, None).collect
        events.length should be(4)

        val exploreContents = events.filter { x => "ExploreContent".equals(x.dimensions.funnel.get) }
        val contentSearch = events.filter { x => "ContentSearch".equals(x.dimensions.funnel.get) }

        exploreContents.length should be(3)
        contentSearch.length should be(1)

        val exploreContent = exploreContents.head

        exploreContent.dimensions.onboarding.get should be(false)
        val eksMap = exploreContent.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.contains("listContent") should be(true)
        eksMap.contains("selectContent") should be(true)
        eksMap.get("timeSpent").get.asInstanceOf[Double] should be(68.57)

    }

    it should "generates funnel summary, from a data having one funnel in each session" in {

        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data3.log");
        val events = GenieFunnelModel.execute(rdd, None).collect
        events.length should be(2)

        val onb = events.filter { x => "GenieOnboarding".equals(x.dimensions.funnel.get) }
        val exp = events.filter { x => "ExploreContent".equals(x.dimensions.funnel.get) }

        onb.length should be(1)
        exp.length should be(1)

        val e1 = onb.last
        val e2 = exp.last

        e1.dimensions.onboarding.get should be(true)
        e2.dimensions.onboarding.get should be(false)

        e1.dimensions.sid.get should not be (e2.dimensions.sid.get)

        val eksMap1 = e1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap1.get("timeSpent").get should be(4.47)

        eksMap1.contains("welcomeContentSkipped") should be(true)
        eksMap1.contains("addChildSkipped") should be(true)
        eksMap1.contains("gotoLibrarySkipped") should be(true)
        eksMap1.contains("firstLessonSkipped") should be(true)
    }

    it should "generates funnel summary, from a data having multiple funnel in multiple session and having onboarding funnel" in {
        val rdd = loadFile[Event]("src/test/resources/genie-funnel/genie-funnel-data4.log");
        val events = GenieFunnelModel.execute(rdd, None).collect
        events.length should be(8)

        val onbEvents = events.filter { x => x.dimensions.onboarding.get == true }

        onbEvents.length should be(events.length)

        onbEvents.head.dimensions.sid.get should not be (onbEvents.last.dimensions.sid.get)
    }
}