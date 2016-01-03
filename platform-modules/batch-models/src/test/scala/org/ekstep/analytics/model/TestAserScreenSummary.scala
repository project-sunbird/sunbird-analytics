package org.ekstep.analytics.model

import org.ekstep.ilimi.analytics.framework.SparkSpec
import java.io.FileWriter
import org.ekstep.ilimi.analytics.framework.JobContext
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.DataFilter
import org.ekstep.ilimi.analytics.framework.Filter
import org.ekstep.ilimi.analytics.framework.util.JSONUtils
import org.ekstep.ilimi.analytics.framework.MeasuredEvent
import org.ekstep.ilimi.analytics.framework.MEEdata
import org.ekstep.ilimi.analytics.framework.MeasuredEvent
import scala.collection.immutable.HashMap.HashTrieMap

class TestAserScreenSummary extends SparkSpec(null) {

    //-----test1-------
    "AserScreenSummary" should "check the correctness of summary events from 'raw.telemetry.test1.json'" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/raw.telemetry.test1.json");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(2)
        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        val sec = JSONUtils.deserialize[MeasuredEvent](me(1)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        first.get("assessNumeracyQ3") should be(Option(0d))
        sec.get("assessNumeracyQ1") should be(Option(0d))
        sec.get("assessNumeracyQ2") should be(Option(0d))
        sec.get("assessNumeracyQ3") should be(Option(0d))
        sec.get("selectNumeracyQ2") should be(Option(0d))
        sec.get("scorecard") should be(Option(0d))
        sec.get("summary") should be(Option(0d))
    }
    //------- test2--------
    it should "check the correctness of summary events from 'raw.telemetry.test2.json'" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/raw.telemetry.test2.json");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(2)
        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        val sec = JSONUtils.deserialize[MeasuredEvent](me(1)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        first.get("assessNumeracyQ3") should be (Option(0d))
        sec.get("assessNumeracyQ3") should be (Option(0d))
    }
    //------- test3--------
    it should "check the correctness of summary events from 'raw.telemetry.test3.json'" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/raw.telemetry.test3.json");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(1)
    }
    //------- test4--------
    it should "check the correctness of summary events from 'raw.telemetry.test4.json'" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/raw.telemetry.test4.json");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(1)
    }
    //------- test5--------
    it should "check the correctness of summary events from 'raw.telemetry.test5.json'" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/raw.telemetry.test5.json");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(2)

        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        val sec = JSONUtils.deserialize[MeasuredEvent](me(1)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        first.get("assessNumeracyQ3") should be(Option(0d))

        sec.get("selectNumeracyQ2") should be(Option(0d))
        sec.get("assessNumeracyQ2") should be(Option(0d))
        sec.get("assessNumeracyQ3") should be(Option(0d))
        sec.get("scorecard") should be(Option(0d))
        sec.get("summary") should be(Option(0d))
    }
    //------- test6--------
    it should "check the correctness of summary events from 'raw.telemetry.test6.json'" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/raw.telemetry.test6.json");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(4)
        
        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        val sec = JSONUtils.deserialize[MeasuredEvent](me(1)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        val fourth = JSONUtils.deserialize[MeasuredEvent](me(3)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];

        first.get("assessNumeracyQ3") should be(Option(0d))
        sec.get("assessNumeracyQ3") should be(Option(0d))

        fourth.get("childReg1") should be(Option(0d))
        fourth.get("childReg2") should be(Option(0d))
        fourth.get("childReg3") should be(Option(0d))
        fourth.get("assessLanguage") should be(Option(0d))
        fourth.get("languageLevel") should be(Option(0d))
        fourth.get("selectNumeracyQ1") should be(Option(0d))
        fourth.get("assessNumeracyQ1") should be(Option(0d))
        fourth.get("selectNumeracyQ2") should be(Option(0d))
        fourth.get("assessNumeracyQ2") should be(Option(0d))
        fourth.get("assessNumeracyQ3") should be(Option(0d))
        fourth.get("scorecard") should be(Option(0d))
        fourth.get("summary") should be(Option(0d))
    }
    //-----test7--
    it should "check summary events , all having non-zero value" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/allAserEventsTest.txt");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(2)

        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        val sec = JSONUtils.deserialize[MeasuredEvent](me(1)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        first.get("activationKeyPage") should not be (Option(0d))
        first.get("surveyCodePage") should not be (Option(0d))
        first.get("childReg1") should not be (Option(0d))
        first.get("childReg2") should not be (Option(0d))
        first.get("childReg3") should not be (Option(0d))
        first.get("assessLanguage") should not be (Option(0d))
        first.get("languageLevel") should not be (Option(0d))
        first.get("selectNumeracyQ1") should not be (Option(0d))
        first.get("assessNumeracyQ1") should not be (Option(0d))
        first.get("selectNumeracyQ2") should not be (Option(0d))
        first.get("assessNumeracyQ2") should not be (Option(0d))
        first.get("assessNumeracyQ3") should not be (Option(0d))
        first.get("scorecard") should not be (Option(0d))
        first.get("summary") should not be (Option(0d))

        sec.get("activationKeyPage") should not be (Option(0d))
        sec.get("surveyCodePage") should not be (Option(0d))
        sec.get("childReg1") should not be (Option(0d))
        sec.get("childReg2") should not be (Option(0d))
        sec.get("childReg3") should not be (Option(0d))
        sec.get("assessLanguage") should not be (Option(0d))
        sec.get("languageLevel") should not be (Option(0d))
        sec.get("selectNumeracyQ1") should not be (Option(0d))
        sec.get("assessNumeracyQ1") should not be (Option(0d))
        sec.get("selectNumeracyQ2") should not be (Option(0d))
        sec.get("assessNumeracyQ2") should not be (Option(0d))
        sec.get("assessNumeracyQ3") should not be (Option(0d))
        sec.get("scorecard") should not be (Option(0d))
        sec.get("summary") should not be (Option(0d))
    }
    //-----test8--
    it should "check summary events, not having any reg. pages" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/noRegPages.txt");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(1)
        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        first.get("activationKeyPage") should be (Option(0d))
        first.get("surveyCodePage") should be (Option(0d))
        first.get("childReg1") should be (Option(0d))
        first.get("childReg2") should be (Option(0d))
        first.get("childReg3") should be (Option(0d))
    }
     //-----test9--
    it should "check summary events, having three reg. pages" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/3nextButton.txt");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(1)
        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        first.get("activationKeyPage") should not be (Option(0d))
        first.get("surveyCodePage") should not be (Option(0d))
        first.get("childReg1") should not be (Option(0d))
        first.get("childReg2") should be (Option(0d))
        first.get("childReg3") should be (Option(0d))
    }
     //-----test10--
    it should "check summary events, only having four assess pages" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/OE_ASSESS.txt");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(1)
        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        first.get("activationKeyPage") should be (Option(0d))
        first.get("surveyCodePage") should be (Option(0d))
        first.get("childReg1") should be (Option(0d))
        first.get("childReg2") should be (Option(0d))
        first.get("childReg3") should be (Option(0d))
        first.get("assessLanguage") should not be (Option(0d))
        first.get("languageLevel") should be (Option(0d))
        first.get("selectNumeracyQ1") should be (Option(0d))
        first.get("assessNumeracyQ1") should not be (Option(0d))
        first.get("selectNumeracyQ2") should be (Option(0d))
        first.get("assessNumeracyQ2") should not be (Option(0d))
        first.get("assessNumeracyQ3") should not be (Option(0d))
        first.get("scorecard") should be (Option(0d))
        first.get("summary") should be (Option(0d))
    }
    
    //-----test11--
    it should "check summary events, only having three Reg. pages" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/only3NextButtonPressed.txt");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(1)
        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        first.get("activationKeyPage") should not be (Option(0d))
        first.get("surveyCodePage") should not be (Option(0d))
        first.get("childReg1") should not be (Option(0d))
        first.get("childReg2") should be (Option(0d))
        first.get("childReg3") should be (Option(0d))
        first.get("assessLanguage") should be (Option(0d))
        first.get("languageLevel") should be (Option(0d))
        first.get("selectNumeracyQ1") should be (Option(0d))
        first.get("assessNumeracyQ1") should be (Option(0d))
        first.get("selectNumeracyQ2") should be (Option(0d))
        first.get("assessNumeracyQ2") should be (Option(0d))
        first.get("assessNumeracyQ3") should be (Option(0d))
        first.get("scorecard") should be (Option(0d))
        first.get("summary") should be (Option(0d))
    }
    //-----test12--
    it should "check summary events, only having all Reg. pages" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/only5NextButtonPressed.txt");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(1)
        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        first.get("activationKeyPage") should not be (Option(0d))
        first.get("surveyCodePage") should not be (Option(0d))
        first.get("childReg1") should not be (Option(0d))
        first.get("childReg2") should not be (Option(0d))
        first.get("childReg3") should not be (Option(0d))
        first.get("assessLanguage") should be (Option(0d))
        first.get("languageLevel") should be (Option(0d))
        first.get("selectNumeracyQ1") should be (Option(0d))
        first.get("assessNumeracyQ1") should be (Option(0d))
        first.get("selectNumeracyQ2") should be (Option(0d))
        first.get("assessNumeracyQ2") should be (Option(0d))
        first.get("assessNumeracyQ3") should be (Option(0d))
        first.get("scorecard") should be (Option(0d))
        first.get("summary") should be (Option(0d))
    }
    //-----test13--
    it should "check two summary events, all having zero field value" in {
        val aserScreener = new AserScreenSummary();
        val event = loadFile("src/test/resources/aserlite-screen-summary/twoOE_START_only.txt");
        val rdd = DataFilter.filter(event, Filter("eventId", "IN", Option(List("OE_START", "OE_INTERACT", "OE_ASSESS", "OE_LEVEL_SET", "OE_END"))));
        val rdd2 = aserScreener.execute(sc, rdd, Option(Map("modelVersion" -> "1.1", "modelId" -> "AserScreenerSummary")));
        val me = rdd2.collect();
        me.length should be(2)
        val first = JSONUtils.deserialize[MeasuredEvent](me(0)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        val sec = JSONUtils.deserialize[MeasuredEvent](me(1)).edata.eks.asInstanceOf[HashTrieMap[String, Double]];
        
        first.get("activationKeyPage") should be (Option(0d))
        first.get("surveyCodePage") should be (Option(0d))
        first.get("childReg1") should be (Option(0d))
        first.get("childReg2") should be (Option(0d))
        first.get("childReg3") should be (Option(0d))
        first.get("assessLanguage") should be (Option(0d))
        first.get("languageLevel") should be (Option(0d))
        first.get("selectNumeracyQ1") should be (Option(0d))
        first.get("assessNumeracyQ1") should be (Option(0d))
        first.get("selectNumeracyQ2") should be (Option(0d))
        first.get("assessNumeracyQ2") should be (Option(0d))
        first.get("assessNumeracyQ3") should be (Option(0d))
        first.get("scorecard") should be (Option(0d))
        first.get("summary") should be (Option(0d))
        
        sec.get("activationKeyPage") should be (Option(0d))
        sec.get("surveyCodePage") should be (Option(0d))
        sec.get("childReg1") should be (Option(0d))
        sec.get("childReg2") should be (Option(0d))
        sec.get("childReg3") should be (Option(0d))
        sec.get("assessLanguage") should be (Option(0d))
        sec.get("languageLevel") should be (Option(0d))
        sec.get("selectNumeracyQ1") should be (Option(0d))
        sec.get("assessNumeracyQ1") should be (Option(0d))
        sec.get("selectNumeracyQ2") should be (Option(0d))
        sec.get("assessNumeracyQ2") should be (Option(0d))
        sec.get("assessNumeracyQ3") should be (Option(0d))
        sec.get("scorecard") should be (Option(0d))
        sec.get("summary") should be (Option(0d))
    }
    //--------
}