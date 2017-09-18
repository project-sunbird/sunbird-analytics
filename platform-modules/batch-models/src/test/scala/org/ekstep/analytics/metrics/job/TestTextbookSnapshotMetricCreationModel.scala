package org.ekstep.analytics.metrics.job

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent
import org.joda.time.DateTime
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.updater.UpdateTextbookSnapshotDB
import org.ekstep.analytics.model.SparkGraphSpec

class TestTextbookSnapshotMetricCreationModel extends SparkGraphSpec(null) {

    "TextbookSnapshotMetricCreationModel" should "execute TextbookSnapshotMetricCreationModel successfully" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_creation_metrics_db.textbook_snapshot_metrics");
        }
        
        val createTextbookQueries = List("""CREATE (n:domain {code:"org.ekstep.textbook.1489083137114", subject:"MATHS", description:"Sample Test bookedition:Edition 1", language:["English"], mimeType:"application/vnd.ekstep.content-collection", medium:"Gujarati", idealScreenSize:"normal", createdOn:"2017-03-09T18:12:09.054+0000", appIcon:"https://ekstep-public-qa.s3-ap-south-1.amazonaws.com/content/banana_268_1472808995_1472809119909.png", gradeLevel:["Grade 2"], publication:"Test Publishers", lastUpdatedOn:"2017-03-09T18:14:07.449+0000", contentType:"Textbook", lastUpdatedBy:"268", visibility:"Default", os:["All"], IL_SYS_NODE_TYPE:"DATA_NODE", author:"Ilimi", mediaType:"content", osId:"org.ekstep.quiz.app", ageGroup:["5-6"], createdBy: "291", versionKey:"1489083247449", idealScreenDensity:"hdpi", compatibilityLevel:"1", IL_FUNC_OBJECT_TYPE:"Content", name:"Sample Test book", IL_UNIQUE_ID:"do_212198568993210368181", board:"CBSE", status: "Draft"}) return n""",
            """CREATE (n:domain {code: "org.ekstep.textbook.1489145488965", subject: "Maths", description: "Birds are in vast variety", edition: "", language: ["English"], mimeType: "application/vnd.ekstep.content-collection", medium: "Gujarati", idealScreenSize: "normal", createdOn: "2017-03-10T11:31:21.222+0000", appIcon: "https://ekstep-public-qa.s3-ap-south-1.amazonaws.com/content/do_212198323238887424167/artifact/c2e10dae47204d873c0c3953b312974a_1489053129759.jpeg", gradeLevel: ["Kindergarten"], publication: "", lastUpdatedOn: "2017-03-20T06:15:40.229+0000", contentType: "Textbook", creator: "Sourav", lastUpdatedBy: "80", visibility: "Default", os: ["All"], IL_SYS_NODE_TYPE: "DATA_NODE", author: "", createdBy: "291", mediaType: "content", osId: "org.ekstep.quiz.app", ageGroup: ["5-6"], versionKey: "1489990540229", idealScreenDensity: "hdpi", compatibilityLevel: 1, IL_FUNC_OBJECT_TYPE: "Content", name: "Birds", IL_UNIQUE_ID: "do_21219907978217062412", board: "NCERT", status: "Draft"}) return n""");
        executeQueries(createTextbookQueries);
        val updateAppIdChannel = List("MATCH (n:domain) SET n.appId='genie', n.channel='in.ekstep'")
	      executeQueries(updateAppIdChannel);
        
        val start_date = DateTime.now().toString(CommonUtil.dateFormat)
        UpdateTextbookSnapshotDB.execute(sc.emptyRDD[DerivedEvent], Option(Map("periodType" -> "ALL", "periodUpTo" -> 100.asInstanceOf[AnyRef])));

        val data = sc.parallelize(List(""))
        val rdd2 = TextbookSnapshotMetricCreationModel.execute(data, Option(Map("start_date" -> start_date.asInstanceOf[AnyRef], "end_date" -> start_date.asInstanceOf[AnyRef])));
        
        rdd2.count() should be(2)
    }
}