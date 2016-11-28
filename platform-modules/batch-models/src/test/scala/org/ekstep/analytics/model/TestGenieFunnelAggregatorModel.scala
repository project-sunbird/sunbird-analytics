package org.ekstep.analytics.model

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils

class TestGenieFunnelAggregatorModel extends SparkSpec(null) {

    "GenieFunnelAggregatorModel" should "generates aggregated funnel summary" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-funnel-aggregator/gfagg-data.log");
        val events = GenieFunnelAggregatorModel.execute(rdd, None).collect
        events.length should be(10)
    }
    it should "generates agregated event and pass the test cases for the input of two funnel with all stage invoked" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-funnel-aggregator/gfagg-data1.log");
        val events = GenieFunnelAggregatorModel.execute(rdd, None).collect
        events.length should be(2)

        val first = events.head
        first.dimensions.funnel.get should be("ContentSearch")
        val fMap = first.edata.eks.asInstanceOf[Map[String, AnyRef]]
        //fMap.get("completionPercentage").get.asInstanceOf[Double] should be(100)
        fMap.map { x =>
            if (x._2.isInstanceOf[Map[String, AnyRef]]) {
                val stages = x._2.asInstanceOf[Map[String, AnyRef]]
                stages.get("dropoffPercentage").get.asInstanceOf[Double] should be(0.0)
            }
        }

        val sec = events.last
        sec.dimensions.funnel.get should be("ExploreContent")

    }

    it should "pass the test cases for the inputs of all the funnel with some stage invoked randomly" in {

        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-funnel-aggregator/gfagg-data2.log");
        val events = GenieFunnelAggregatorModel.execute(rdd, None).collect

        events.map { x => x.mid }.distinct.length should be(events.length)

        val exCont = events.filter { x => "ExploreContent".equals(x.dimensions.funnel.get) && "e21cced8318f585b79444879d1578deae4038a4e".equals(x.dimensions.did.get) }.last

        val eksMap = exCont.edata.eks.asInstanceOf[Map[String, AnyRef]]
        //eksMap.get("completionPercentage").get.asInstanceOf[Double] should be > (0.0)

        val cpStage = eksMap.get("contentPlayed").get.asInstanceOf[StageAggSumm]
        cpStage.timeSpent should be(235.75)
        cpStage.dropoffPercentage should not be (100.0)

        eksMap.get("totalTimeSpent").get.asInstanceOf[Double] should be(253.0)
    }

    it should "test for the input of some of the funnel with some of the stages invoked in one device ID" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-funnel-aggregator/gfagg-data3.log");
        val events = GenieFunnelAggregatorModel.execute(rdd, None).collect
        events.length should be(2)

        val eXc = events.filter { x => "ExploreContent".equals(x.dimensions.funnel.get) }.last
        val cs = events.filter { x => "ContentSearch".equals(x.dimensions.funnel.get) }.last

        val eXcEksMap = eXc.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val csEksMap = cs.edata.eks.asInstanceOf[Map[String, AnyRef]]

//        val completionPercentage = eXcEksMap.get("completionPercentage").get.asInstanceOf[Double]
//        completionPercentage should be > (0.0)
//        completionPercentage should be < (50.0)

        eXcEksMap.get("totalTimeSpent").get.asInstanceOf[Double] should be(331.25)

        //val cpcs = csEksMap.get("completionPercentage").get.asInstanceOf[Double]
        //cpcs should be > (0.0)
        //cpcs should be(50.0)
        val cplCS = csEksMap.get("contentPlayed").get.asInstanceOf[StageAggSumm]
        //cplCS.dropoffPercentage should be(100 - cpcs)

        csEksMap.get("totalTimeSpent").get.asInstanceOf[Double] should be(186.1)
    }
}