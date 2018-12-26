package org.ekstep.analytics.model

import org.ekstep.analytics.adapter.ContentFetcher
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.scalamock.scalatest.MockFactory
import org.scalatest._

class TestETBCoverageSummaryModel extends FlatSpec with Matchers with MockFactory {

    val sc = CommonUtil.getSparkContext(1, "TestAnalyticsCore")

    "getLastPublishedContents Method" should "fetch last published contents" in {
        val config = Map("fromDate" -> "2018-11-09", "toDate" -> "2018-11-09")
        val mockContentAdapter = mock[ContentFetcher]

        (mockContentAdapter.getTextbookContents _).expects(config("fromDate")+"T00:00:00.000+05:30", config("toDate")+"T23:59:59.000+05:30")
            .returns(Array(Map("identifier" -> "do_1126053858324398081174",
                    "objectType" -> "Content"),
                Map("identifier" -> "do_1125253005997834241549",
                    "objectType" -> "Content")))

        val contentIds = ETBCoverageSummaryModel.getPublishedTextbooks(config, mockContentAdapter)

        contentIds.size should be(2)
        contentIds.head should be("do_1126053858324398081174")
        contentIds.last should be("do_1125253005997834241549")
    }

    it should "return empty list when no results from API" in {
        val config = Map("fromDate" -> "2018-11-09", "toDate" -> "2018-11-09")
        val mockContentAdapter = mock[ContentFetcher]

        (mockContentAdapter.getTextbookContents _).expects(config("fromDate")+"T00:00:00.000+05:30", config("toDate")+"T23:59:59.000+05:30")
            .returns(Array())

        val contentIds = ETBCoverageSummaryModel.getPublishedTextbooks(config, mockContentAdapter)

        contentIds.size should be(0)
    }

    "computeMetrics Method" should "flatten the hierarchy and generate metrics" in {
        val data = sc.textFile("src/test/resources/etb-coverage-summary/hierarchy_content_do_312522397435895808114830.log")
        val HierarchyModelRDD = data.map(JSONString => JSONUtils.deserialize[ContentHierarchyModel](JSONString))

        val result = ETBCoverageSummaryModel.computeMetrics(HierarchyModelRDD)

        val metrics = result.collect()
        metrics.length should be(10)

        val level1 = metrics.filter(_.getOrElse("level", 0).asInstanceOf[Int] == 1)
        level1.length should be(1)
        level1.head.getOrElse("totalDialcodeAttached", 0).asInstanceOf[Int] should be(2)
        level1.head.getOrElse("totalDialcode", Map[String, Int]()).asInstanceOf[Map[String, Int]] should be(Map("DHUYDW" -> 1, "DI4UFJ" -> 1))
        level1.head.getOrElse("totalDialcodeLinkedToContent", 0).asInstanceOf[Int] should be(2)
        level1.head.getOrElse("mimeType", "").asInstanceOf[String] should be("application/vnd.ekstep.content-collection")

        val level2 = metrics.filter(_.getOrElse("level", 0).asInstanceOf[Int] == 2)
        level2.length should be(3)
        val content1 = level2.find(_.getOrElse("id", "").asInstanceOf[String].equals("do_312522403304308736114833")).getOrElse(Map())
        content1.get("totalDialcode") match {
            case Some(dialcodes) => dialcodes should be(Map())
        }
        content1.get("totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked should be(0)
        }
        content1.get("totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached should be(0)
        }
        content1.get("mimeType") match {
            case Some(mimeType) => mimeType should be("application/vnd.ekstep.content-collection")
        }
        val content2 = level2.find(_.getOrElse("id", "").asInstanceOf[String].equals("do_312522403304308736114835")).getOrElse(Map())
        content2.get("totalDialcode") match {
            case Some(dialcodes) => dialcodes should be(Map())
        }
        content2.get("totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked should be(0)
        }
        content2.get("totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached should be(0)
        }
        content2.get("mimeType") match {
            case Some(mimeType) => mimeType should be("application/vnd.ekstep.content-collection")
        }
        val content3 = level2.find(_.getOrElse("id", "").asInstanceOf[String].equals("do_312522403304308736114834")).getOrElse(Map())
        content3.get("totalDialcode") match {
            case Some(dialcodes) => dialcodes should be(Map("DI4UFJ" -> 1))
        }
        content3.get("totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked should be(1)
        }
        content3.get("totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached should be(1)
        }
        content3.get("mimeType") match {
            case Some(mimeType) => mimeType should be("application/vnd.ekstep.content-collection")
        }

        val level3 = metrics.filter(_.getOrElse("level", 0).asInstanceOf[Int] == 3)
        level3.length should be(2)

        val content3_1 = level3.find(_.getOrElse("id", "").asInstanceOf[String].equals("do_312522403304316928114836")).getOrElse(Map())
        content3_1.get("totalDialcode") match {
            case Some(dialcodes) => dialcodes should be(Map("DIDQH7" -> 1, "DIMLIU" -> 1))
        }
        content3_1.get("totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked should be(2)
        }
        content3_1.get("totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached should be(2)
        }
        content3_1.get("mimeType") match {
            case Some(mimeType) => mimeType should be("application/vnd.ekstep.content-collection")
        }

        val content3_2 = level3.find(_.getOrElse("id", "").asInstanceOf[String].equals("do_312522403304325120114842")).getOrElse(Map())
        content3_2.get("totalDialcode") match {
            case Some(dialcodes) => dialcodes should be(Map("DUI3W4" -> 1,"DU97UG" -> 1))
        }
        content3_2.get("totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked should be(2)
        }
        content3_2.get("totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached should be(2)
        }
        content3_2.get("mimeType") match {
            case Some(mimeType) => mimeType should be("application/vnd.ekstep.content-collection")
        }

        val level4 = metrics.filter(_.getOrElse("level", 0).asInstanceOf[Int] == 4)
        level4.length should be(4)
        val content4_1 = level4.find(_.getOrElse("id", "").asInstanceOf[String].equals("do_312522403304349696114859")).getOrElse(Map())
        content4_1.get("totalDialcode") match {
            case Some(dialcodes) => dialcodes should be(Map("DUI3W4" -> 1))
        }
        content4_1.get("totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked should be(1)
        }
        content4_1.get("totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached should be(1)
        }
        content4_1.get("mimeType") match {
            case Some(mimeType) => mimeType should be("application/vnd.ekstep.content-collection")
        }

        val content4_2 = level4.find(_.getOrElse("id", "").asInstanceOf[String].equals("do_312522403304349696114858")).getOrElse(Map())
        content4_2.get("totalDialcode") match {
            case Some(dialcodes) => dialcodes should be(Map("DU97UG" -> 1))
        }
        content4_2.get("totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked should be(1)
        }
        content4_2.get("totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached should be(1)
        }
        content4_2.get("mimeType") match {
            case Some(mimeType) => mimeType should be("application/vnd.ekstep.content-collection")
        }

        val content4_3 = level4.find(_.getOrElse("id", "").asInstanceOf[String].equals("do_312522403304333312114847")).getOrElse(Map())
        content4_3.get("totalDialcode") match {
            case Some(dialcodes) => dialcodes should be(Map("DIMLIU" -> 1))
        }
        content4_3.get("totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked should be(1)
        }
        content4_3.get("totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached should be(1)
        }
        content4_3.get("mimeType") match {
            case Some(mimeType) => mimeType should be("application/vnd.ekstep.content-collection")
        }

        val content4_4 = level4.find(_.getOrElse("id", "").asInstanceOf[String].equals("do_312522403304333312114846")).getOrElse(Map())
        content4_4.get("totalDialcode") match {
            case Some(dialcodes) => dialcodes should be(Map("DIDQH7" -> 1))
        }
        content4_4.get("totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked should be(1)
        }
        content4_4.get("totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached should be(1)
        }
        content4_4.get("mimeType") match {
            case Some(mimeType) => mimeType should be("application/vnd.ekstep.content-collection")
        }
    }

    "getContentCountLinkedToDialcode Method" should "get count of content linked with a dialcode (level wise)" in {
        val data = sc.textFile("src/test/resources/etb-coverage-summary/hierarchy_content_do_312522397435895808114830.log")
        val HierarchyModelRDD = data.map(JSONString => JSONUtils.deserialize[ContentHierarchyModel](JSONString)).collect()

        val noOfLinkedContent = ETBCoverageSummaryModel.getContentCountLinkedToDialcode(HierarchyModelRDD.head)
        noOfLinkedContent should be(2)
    }

    "getDialcodesByLevel Method" should "get list of dialcodes attached to parent and its immediate children" in {
        val data = sc.textFile("src/test/resources/etb-coverage-summary/hierarchy_content_do_312522397435895808114830.log")
        val HierarchyModelRDD = data.map(JSONString => JSONUtils.deserialize[ContentHierarchyModel](JSONString)).collect()

        val listOfDialcodes = ETBCoverageSummaryModel.getDialcodesByLevel(HierarchyModelRDD.head)
        listOfDialcodes should contain theSameElementsAs List("DI4UFJ", "DHUYDW")
    }

    "postProcess Method" should "generate GraphEvent with metrics" in {
        val data = sc.textFile("src/test/resources/etb-coverage-summary/hierarchy_content_do_312522397435895808114830.log")
        val HierarchyModelRDD = data.map(JSONString => JSONUtils.deserialize[ContentHierarchyModel](JSONString))
        val config = Map("fromDate" -> "2018-11-09", "toDate" -> "2018-11-09")

        val algoOutput = ETBCoverageSummaryModel.algorithm(HierarchyModelRDD, config)(sc)
        val result = ETBCoverageSummaryModel.postProcess(algoOutput, config)(sc)

        val metrics = result.collect()
        metrics.length should be(10)

        val level1 = metrics.filter(_.transactionData.getOrElse("properties", Map()).getOrElse("me_hierarchyLevel", Map()).getOrElse("nv", 0).asInstanceOf[Int] ==1)

        level1.length should be(1)
        level1.head.transactionData.getOrElse("properties", Map()).getOrElse("me_totalDialcodeAttached", Map()).getOrElse("nv", 0).asInstanceOf[Int] should be(2)
        level1.head.transactionData.getOrElse("properties", Map()).getOrElse("me_totalDialcode", Map()).getOrElse("nv", 0).asInstanceOf[Map[String, Int]] should be(Map("DHUYDW" -> 1, "DI4UFJ" -> 1))
        level1.head.transactionData.getOrElse("properties", Map()).getOrElse("me_totalDialcodeLinkedToContent", Map()).getOrElse("nv", 0).asInstanceOf[Int] should be(2)
        level1.head.objectType should be("Content")
        level1.head.nodeType should be("DATA_NODE")
        level1.head.graphId should be("domain")
        level1.head.ets should be < System.currentTimeMillis()
        level1.head.nodeUniqueId should be("do_312522397435895808114830")
        level1.head.operationType should be("UPDATE")

        val level2 = metrics.filter(_.transactionData.getOrElse("properties", Map()).getOrElse("me_hierarchyLevel", Map()).getOrElse("nv", 0).asInstanceOf[Int] == 2)
        level2.length should be(3)
        val content2_1graphEvent = level2.find(_.nodeUniqueId.equals("do_312522403304308736114833")).get
        val content2_1 = content2_1graphEvent.transactionData.getOrElse("properties", Map())
        content2_1.get("me_totalDialcode") match {
            case Some(dialcodes) => dialcodes.getOrElse("nv", Map()) should be(Map())
        }
        content2_1.get("me_totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked.getOrElse("nv", 0) should be(0)
        }
        content2_1.get("me_totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached.getOrElse("nv", 0) should be(0)
        }
        content2_1graphEvent.objectType should be("Content")
        content2_1graphEvent.nodeType should be("DATA_NODE")
        content2_1graphEvent.graphId should be("domain")
        content2_1graphEvent.ets should be < System.currentTimeMillis()
        content2_1graphEvent.operationType should be("UPDATE")

        val content2_2graphEvent = level2.find(_.nodeUniqueId.equals("do_312522403304308736114835")).get
        val content2_2 = content2_2graphEvent.transactionData.getOrElse("properties", Map())
        content2_2.get("me_totalDialcode") match {
            case Some(dialcodes) => dialcodes.getOrElse("nv", Map()) should be(Map())
        }
        content2_2.get("me_totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked.getOrElse("nv", 0) should be(0)
        }
        content2_2.get("me_totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached.getOrElse("nv", 0) should be(0)
        }
        content2_2graphEvent.objectType should be("Content")
        content2_2graphEvent.nodeType should be("DATA_NODE")
        content2_2graphEvent.graphId should be("domain")
        content2_2graphEvent.ets should be < System.currentTimeMillis()
        content2_2graphEvent.operationType should be("UPDATE")

        val content2_3graphEvent = level2.find(_.nodeUniqueId.equals("do_312522403304308736114834")).get
        val content2_3 = content2_3graphEvent.transactionData.getOrElse("properties", Map())
        content2_3.get("me_totalDialcode") match {
            case Some(dialcodes) => dialcodes.getOrElse("nv", Map()) should be(Map("DI4UFJ" -> 1))
        }
        content2_3.get("me_totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked.getOrElse("nv", 0) should be(1)
        }
        content2_3.get("me_totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached.getOrElse("nv", 0) should be(1)
        }
        content2_3graphEvent.objectType should be("Content")
        content2_3graphEvent.nodeType should be("DATA_NODE")
        content2_3graphEvent.graphId should be("domain")
        content2_3graphEvent.ets should be < System.currentTimeMillis()
        content2_3graphEvent.operationType should be("UPDATE")

        val level3 = metrics.filter(_.transactionData.getOrElse("properties", Map()).getOrElse("me_hierarchyLevel", Map()).getOrElse("nv", 0).asInstanceOf[Int] == 3)
        level3.length should be(2)

        val content3_1graphEvent = level3.find(_.nodeUniqueId.equals("do_312522403304316928114836")).get
        val content3_1 = content3_1graphEvent.transactionData.getOrElse("properties", Map())
        content3_1.get("me_totalDialcode") match {
            case Some(dialcodes) => dialcodes.getOrElse("nv", Map()) should be(Map("DIDQH7" -> 1, "DIMLIU" -> 1))
        }
        content3_1.get("me_totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked.getOrElse("nv", 0) should be(2)
        }
        content3_1.get("me_totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached.getOrElse("nv", 0) should be(2)
        }
        content3_1graphEvent.objectType should be("Content")
        content3_1graphEvent.nodeType should be("DATA_NODE")
        content3_1graphEvent.graphId should be("domain")
        content3_1graphEvent.ets should be < System.currentTimeMillis()
        content3_1graphEvent.operationType should be("UPDATE")

        val content3_2graphEvent = level3.find(_.nodeUniqueId.equals("do_312522403304325120114842")).get
        val content3_2 = content3_2graphEvent.transactionData.getOrElse("properties", Map())
        content3_2.get("me_totalDialcode") match {
            case Some(dialcodes) => dialcodes.getOrElse("nv", Map()) should be(Map("DUI3W4" -> 1,"DU97UG" -> 1))
        }
        content3_2.get("me_totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked.getOrElse("nv", 0) should be(2)
        }
        content3_2.get("me_totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached.getOrElse("nv", 0) should be(2)
        }
        content3_2graphEvent.objectType should be("Content")
        content3_2graphEvent.nodeType should be("DATA_NODE")
        content3_2graphEvent.graphId should be("domain")
        content3_2graphEvent.ets should be < System.currentTimeMillis()
        content3_2graphEvent.operationType should be("UPDATE")

        val level4 = metrics.filter(_.transactionData.getOrElse("properties", Map()).getOrElse("me_hierarchyLevel", Map()).getOrElse("nv", 0).asInstanceOf[Int] == 4)
        level4.length should be(4)

        val content4_1graphEvent = level4.find(_.nodeUniqueId.equals("do_312522403304349696114859")).get
        val content4_1 = content4_1graphEvent.transactionData.getOrElse("properties", Map())
        content4_1.get("me_totalDialcode") match {
            case Some(dialcodes) => dialcodes.getOrElse("nv", Map()) should be(Map("DUI3W4" -> 1))
        }
        content4_1.get("me_totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked.getOrElse("nv", 0) should be(1)
        }
        content4_1.get("me_totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached.getOrElse("nv", 0) should be(1)
        }
        content4_1graphEvent.objectType should be("Content")
        content4_1graphEvent.nodeType should be("DATA_NODE")
        content4_1graphEvent.graphId should be("domain")
        content4_1graphEvent.ets should be < System.currentTimeMillis()
        content4_1graphEvent.operationType should be("UPDATE")

        val content4_2graphEvent = level4.find(_.nodeUniqueId.equals("do_312522403304349696114858")).get
        val content4_2 = content4_2graphEvent.transactionData.getOrElse("properties", Map())
        content4_2.get("me_totalDialcode") match {
            case Some(dialcodes) => dialcodes.getOrElse("nv", Map()) should be(Map("DU97UG" -> 1))
        }
        content4_2.get("me_totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked.getOrElse("nv", 0) should be(1)
        }
        content4_2.get("me_totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached.getOrElse("nv", 0) should be(1)
        }
        content4_2graphEvent.objectType should be("Content")
        content4_2graphEvent.nodeType should be("DATA_NODE")
        content4_2graphEvent.graphId should be("domain")
        content4_2graphEvent.ets should be < System.currentTimeMillis()
        content4_2graphEvent.operationType should be("UPDATE")

        val content4_3graphEvent = level4.find(_.nodeUniqueId.equals("do_312522403304333312114847")).get
        val content4_3 = content4_3graphEvent.transactionData.getOrElse("properties", Map())
        content4_3.get("me_totalDialcode") match {
            case Some(dialcodes) => dialcodes.getOrElse("nv", Map()) should be(Map("DIMLIU" -> 1))
        }
        content4_3.get("me_totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked.getOrElse("nv", 0) should be(1)
        }
        content4_3.get("me_totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached.getOrElse("nv", 0) should be(1)
        }
        content4_3graphEvent.objectType should be("Content")
        content4_3graphEvent.nodeType should be("DATA_NODE")
        content4_3graphEvent.graphId should be("domain")
        content4_3graphEvent.ets should be < System.currentTimeMillis()
        content4_3graphEvent.operationType should be("UPDATE")

        val content4_4graphEvent = level4.find(_.nodeUniqueId.equals("do_312522403304333312114846")).get
        val content4_4 = content4_4graphEvent.transactionData.getOrElse("properties", Map())
        content4_4.get("me_totalDialcode") match {
            case Some(dialcodes) => dialcodes.getOrElse("nv", Map()) should be(Map("DIDQH7" -> 1))
        }
        content4_4.get("me_totalDialcodeLinkedToContent") match {
            case Some(dialcodeLinked) => dialcodeLinked.getOrElse("nv", 0) should be(1)
        }
        content4_4.get("me_totalDialcodeAttached") match {
            case Some(dialcodeAttached) => dialcodeAttached.getOrElse("nv", 0) should be(1)
        }
        content4_4graphEvent.objectType should be("Content")
        content4_4graphEvent.nodeType should be("DATA_NODE")
        content4_4graphEvent.graphId should be("domain")
        content4_4graphEvent.ets should be < System.currentTimeMillis()
        content4_4graphEvent.operationType should be("UPDATE")
    }
}
