package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.ml.linalg.{ Vector, Vectors, DenseVector }
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.adapter.ContentModel
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.updater.DeviceSpec

class TestDeviceRecommendationScoringModel extends SparkSpec(null) {
    
    "DeviceRecommendationScoringModel" should "load model with zero pairwise interactions and generate scores" in {

        val mockREScoring = new REScoringMock()
        mockREScoring.execute(null, Option(Map("model_name" -> "fm.model1", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "live_content_limit" -> Int.box(1000), "key" -> "model/test/")))
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }

    it should "load model with pairwise interactions and generate scores" in {

        val mockREScoring = new REScoringMock()
        mockREScoring.execute(null, Option(Map("model_name" -> "fm.model2", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "live_content_limit" -> Int.box(1000), "key" -> "model/test/")))
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }

    it should "load model with zero W0 and generate scores" in {

        val mockREScoring = new REScoringMock()
        mockREScoring.execute(null, Option(Map("model_name" -> "fm.model3", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "live_content_limit" -> Int.box(1000), "key" -> "model/test/")))
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }

    it should "load model with zero unary interactions and generate scores" in {

        val mockREScoring = new REScoringMock()
        mockREScoring.execute(null, Option(Map("model_name" -> "fm.model4", "localPath" -> "src/test/resources/device-recos-training/RE-data/", "live_content_limit" -> Int.box(1000), "key" -> "model/test/")))
        CommonUtil.deleteDirectory("src/test/resources/device-recos-training/RE-data");
    }
}

class REScoringMock {
    
    val x = List(DeviceContext("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","domain_49024",ContentModel("domain_49024",List("literacy"),"Story",List("English")),ContentToVector("domain_49024", None, None)
            ,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","domain_49024",None,None,None,None,Option(20.0),None,None,None,None,None,None,None),cus_t(Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),"domain_68601",
            ContentModel("domain_68601",List("numeracy"),"Worksheet",List("Hindi")),null,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","domain_68601",None,Option(10),Option(20),None,Option(20.0),Option(1452038407000L),Option(1459641600000L),Option(0.0),Option(false),Option(1475731808000L),Option(0),Option(1)),
            ContentUsageSummaryFact(0,"domain_68601","all",new DateTime(0),new DateTime(0),new DateTime(0),20.0,0,0.0,0,0.0,4,0.0,Array()),DeviceUsageSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043",Option(1452038407000L),Option(1475731808000L),Option(410),Option(3),Option(30.0),Option(0.01),Option(0.07),Option(2),Option(1475731808000L),Option(1452038407000L),Option(10.0),Option(1),Option(10.0),Option(0.0),Option("")),
            DeviceSpec("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","","","Android 5.0.1","Samsung S685",-1.0,1.0,1.0,1.0,"1.0,1.0","abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)",1.0,List()),cus_t(Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),dus_tf(Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(0.0),Option(1.0),Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),
            dcus_tf("domain_68601",Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),null),
            DeviceContext("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","Test_QAT_1070",ContentModel("Test_QAT_1070",List("numeracy"),"Worksheet",List("Kannada")),null,
            DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","Test_QAT_1070",None,None,None,None,Option(10.0),None,None,None,None,None,None,None),cus_t(Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0)),"domain_68601",
            ContentModel("domain_68602",List("literacy"),"Story",List("English")),null,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","domain_68601",None,Option(1),Option(10),None,Option(20.0),Option(1452038407000L),Option(1459641600000L),Option(0.0),Option(true),Option(1475731808000L),Option(0),Option(1)),
            ContentUsageSummaryFact(0,"domain_68601","all",new DateTime(0),new DateTime(0),new DateTime(0),20.0,0,0.0,0,0.0,4,0.0,Array()),DeviceUsageSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043",Option(1452038407000L),Option(1475731808000L),Option(410),Option(3),Option(30.0),Option(0.01),Option(0.07),Option(2),Option(1475731808000L),Option(1452038407000L),Option(10.0),Option(1),Option(10.0),Option(0.0),Option("")),
            DeviceSpec("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a043","","","Android 5.0.1","Samsung S685",-1.0,1.0,1.0,1.0,"1.0,1.0","abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)",1.0,List()),cus_t(Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0)),dus_tf(Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0)),
            dcus_tf("domain_68601",Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0),Option(2.0)),null),
            DeviceContext("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","domain_49024",ContentModel("domain_49024",List("literacy"),"Story",List("English")),ContentToVector("domain_49024", None, None)
            ,DeviceContentSummary(null,null,None,None,None,None,None,None,None,None,None,None,None,None),cus_t(None,None,None,None,None,None,None,None,None),"domain_68601",
            ContentModel("domain_68601",List("numeracy"),"Worksheet",List("Hindi")),null,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","domain_68601",None,Option(1),Option(10),None,Option(20.0),Option(1452038407000L),Option(1459641600000L),Option(0.0),Option(false),Option(1475731808000L),Option(0),Option(1)),
            ContentUsageSummaryFact(0,"domain_68601","all",new DateTime(0),new DateTime(0),new DateTime(0),20.0,0,0.0,0,0.0,4,0.0,Array()),DeviceUsageSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044",Option(1452038407000L),Option(1475731808000L),Option(410),Option(3),Option(30.0),Option(0.01),Option(0.07),Option(2),Option(1475731808000L),Option(1452038407000L),Option(10.0),Option(1),Option(10.0),Option(0.0),Option("")),
            DeviceSpec("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","","","Android 5.0.1","Micromax A065",-1.0,2.0,2.0,2.0,"2.0,2.0","abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)",1.0,List()),cus_t(Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),dus_tf(Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(0.0),Option(1.0),Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),
            dcus_tf(null,None,None,None,None,None,None,None,None,None,None),null),
            DeviceContext("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","Test_QAT_1070",ContentModel("Test_QAT_1070",List("numeracy"),"Worksheet",List("Kannada")),null,
            DeviceContentSummary(null,null,None,None,None,None,None,None,None,None,None,None,None,None),cus_t(None,None,None,None,None,None,None,None,None),"domain_68601",
            ContentModel("domain_68602",List("literacy"),"Story",List("English")),null,DeviceContentSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","domain_68601",None,Option(1),Option(10),None,Option(20.0),Option(1452038407000L),Option(1459641600000L),Option(0.0),Option(false),Option(1475731808000L),Option(0),Option(1)),
            ContentUsageSummaryFact(0,"domain_68601","all",new DateTime(0),new DateTime(0),new DateTime(0),20.0,0,0.0,0,0.0,4,0.0,Array()),DeviceUsageSummary("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044",Option(1452038407000L),Option(1475731808000L),Option(410),Option(3),Option(30.0),Option(0.01),Option(0.07),Option(2),Option(1475731808000L),Option(1452038407000L),Option(10.0),Option(1),Option(10.0),Option(0.0),Option("")),
            DeviceSpec("9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a044","","","Android 5.0.1","Micromax A065",-1.0,2.0,2.0,2.0,"2.0,2.0","abi: armeabi-v7a  ARMv7 Processor rev 4 (v7l)",1.0,List()),cus_t(Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),dus_tf(Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0),Option(0.0),Option(1.0),Option(0.0),Option(1.0),Option(1.0),Option(1.0),Option(1.0)),
            dcus_tf(null,None,None,None,None,None,None,None,None,None,None),null)) 
            
    import org.ekstep.analytics.model.DeviceRecommendationScoringModel
    def preProcess()(implicit sc: SparkContext): RDD[DeviceContext] = {
        sc.makeRDD(x);
    }
    def execute(events: RDD[DerivedEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[DeviceRecos] = {
        
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val inputRDD = preProcess();
        JobContext.recordRDD(inputRDD);
        val outputRDD = DeviceRecommendationScoringModel.algorithm(inputRDD, config);
        JobContext.recordRDD(outputRDD);
        val resultRDD = DeviceRecommendationScoringModel.postProcess(outputRDD, config);
        JobContext.recordRDD(resultRDD);
        resultRDD
    }
}