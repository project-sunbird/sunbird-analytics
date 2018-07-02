package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DerivedEvent
import org.joda.time.DateTimeUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.util.Constants
import org.scalatest.Ignore

/**
 * @author Mahesh Kumar Gangula
 */
@Ignore
class TestUpdateTextbookSnapshotDB extends SparkGraphSpec(null) {

	
	"UpdateTextbookSnapshotDB" should "return zero snapshots" in {
		val rdd = invokeSnapshotUpdater;
		rdd.count should be(0);
	}
	
	it should "return zero textbookunits for empty textbook" in {
		deleteTextbookData;
		createTextbooks;
		updateAppIdChannel;
		val rdd = invokeSnapshotUpdater;
		rdd.count should be(6);
		val snapshot = rdd.first();
		(snapshot.textbookunit_count) should be(0);
	}
	
	it should "return zero content_count for textbook having empty bookunits" in {
		deleteTextbookData;
		createTextbooks;
		createBookunits;
		val rdd = invokeSnapshotUpdater;
		rdd.count should be(6);
		val snapshot = rdd.first();
		(snapshot.textbookunit_count>0) should be(true);
		(snapshot.content_count) should be(0);
		
	}
	
	it should "return textbook snapshot metrics for textbook having bookunits and subunits(contents)" in {
		val stories = List(AppObjectCache("Content", "do_212198568993210368181", "genie", "in.ekstep", Option("Story"), None, None, None, "Test Story 1", "Draft", Option(""), None), AppObjectCache("Content", "do_21219907978217062412", "genie", "in.ekstep", Option("Story"), None, None, None, "Test Story 2", "Draft", Option(""), None));
		sc.parallelize(stories, JobContext.parallelization).saveToCassandra(Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE)
		deleteTextbookData;
		createTextbooks;
		createBookunits;
		linkBookUnitsWithContents;
		val rdd = invokeSnapshotUpdater;
		rdd.count should be(6);
		val snapshot = rdd.first();
		(snapshot.textbookunit_count>0) should be(true);
		(snapshot.content_count>0) should be(true);
	}
	
	private def invokeSnapshotUpdater(): RDD[TextbookSnapshotSummary] = {
		val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/influxDB-updater/template.json")), Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/influxDB-updater/asset.json"))))), None, None, "org.ekstep.analytics.updater.ConsumptionMetricsUpdater", Option(Map("periodType" -> "ALL", "periodUpTo" -> 100.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Consumption Metrics Updater"), Option(false))
		UpdateTextbookSnapshotDB.execute(sc.emptyRDD[DerivedEvent], config.modelParams);
	}
	
	private def deleteTextbookData() {
		executeQueries(List("MATCH (n:domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE n.contentType IN ['Textbook', 'TextBookUnit'] DETACH DELETE n"));
	}
	
	private def createTextbooks() {
	val queries = List("""CREATE (n:domain {code:"org.ekstep.textbook.1489083137114", subject:"MATHS", description:"Sample Test bookedition:Edition 1", language:["English"], mimeType:"application/vnd.ekstep.content-collection", medium:"Gujarati", idealScreenSize:"normal", createdOn:"2017-03-09T18:12:09.054+0000", appIcon:"https://ekstep-public-qa.s3-ap-south-1.amazonaws.com/content/banana_268_1472808995_1472809119909.png", gradeLevel:["Grade 2"], publication:"Test Publishers", lastUpdatedOn:"2017-03-09T18:14:07.449+0000", contentType:"Textbook", lastUpdatedBy:"268", visibility:"Default", os:["All"], IL_SYS_NODE_TYPE:"DATA_NODE", author:"Ilimi", mediaType:"content", osId:"org.ekstep.quiz.app", ageGroup:["5-6"], createdBy: "291", versionKey:"1489083247449", idealScreenDensity:"hdpi", compatibilityLevel:"1", IL_FUNC_OBJECT_TYPE:"Content", name:"Sample Test book", IL_UNIQUE_ID:"do_212198568993210368181", board:"CBSE", status: "Draft"}) return n""",
		"""CREATE (n:domain {code: "org.ekstep.textbook.1489145488965", subject: "Maths", description: "Birds are in vast variety", edition: "", language: ["English"], mimeType: "application/vnd.ekstep.content-collection", medium: "Gujarati", idealScreenSize: "normal", createdOn: "2017-03-10T11:31:21.222+0000", appIcon: "https://ekstep-public-qa.s3-ap-south-1.amazonaws.com/content/do_212198323238887424167/artifact/c2e10dae47204d873c0c3953b312974a_1489053129759.jpeg", gradeLevel: ["Kindergarten"], publication: "", lastUpdatedOn: "2017-03-20T06:15:40.229+0000", contentType: "Textbook", creator: "Sourav", lastUpdatedBy: "80", visibility: "Default", os: ["All"], IL_SYS_NODE_TYPE: "DATA_NODE", author: "", createdBy: "291", mediaType: "content", osId: "org.ekstep.quiz.app", ageGroup: ["5-6"], versionKey: "1489990540229", idealScreenDensity: "hdpi", compatibilityLevel: 1, IL_FUNC_OBJECT_TYPE: "Content", name: "Birds", IL_UNIQUE_ID: "do_21219907978217062412", board: "NCERT", status: "Draft"}) return n""");
	executeQueries(queries);
	}
	
	private def createBookunits() {
		val queries = List("""CREATE (n:domain {pageNumber: "", code: "org.ekstep.textbook.1488815091031", notes: "", visibility: "Parent", os: ["All"], IL_SYS_NODE_TYPE: "DATA_NODE", description: "aqsdwds", mediaType: "content", language: ["English"], mimeType: "application/vnd.ekstep.content-collection", osId: "org.ekstep.quiz.app", ageGroup: ["5-6"], idealScreenSize: "normal", createdOn: "2017-03-06T15:44:53.151+0000", versionKey: "1488815156263", idealScreenDensity: "hdpi", gradeLevel: ["Grade 1"], compatibilityLevel: 1, IL_FUNC_OBJECT_TYPE: "Content", name: "test book 2", lastUpdatedOn: "2017-03-06T15:45:56.263+0000", contentType: "TextBookUnit", IL_UNIQUE_ID: "do_112196373243092992119", status: "Draft"}) return n""",
			"""CREATE (n:domain {pageNumber: "", code: "org.ekstep.textbook.1488815148233", notes: "", visibility: "Parent", os: ["All"], IL_SYS_NODE_TYPE: "DATA_NODE", description: "sds", mediaType: "content", language: ["English"], mimeType: "application/vnd.ekstep.content-collection", osId: "org.ekstep.quiz.app", ageGroup: ["5-6"], idealScreenSize: "normal", createdOn: "2017-03-06T15:45:50.319+0000", versionKey: "1488815150319", idealScreenDensity: "hdpi", gradeLevel: ["Grade 1"], compatibilityLevel: 1, IL_FUNC_OBJECT_TYPE: "Content", name: "tezst 3szx", lastUpdatedOn: "2017-03-06T15:45:50.319+0000", contentType: "TextBookUnit", IL_UNIQUE_ID: "do_112196373711413248120", status: "Draft"}) return n""",
			"""CREATE (n:domain {pageNumber: "", code: "org.ekstep.textbook.1488815361239", notes: "", visibility: "Parent", os: ["All"], IL_SYS_NODE_TYPE: "DATA_NODE", description: "Top Unit 1 ", mediaType: "content", language: ["English"], mimeType: "application/vnd.ekstep.content-collection", osId: "org.ekstep.quiz.app", ageGroup: ["5-6"], idealScreenSize: "normal", createdOn: "2017-03-06T15:49:23.343+0000", versionKey: "1488815363343", idealScreenDensity: "hdpi", gradeLevel: ["Grade 1"], compatibilityLevel: 1, IL_FUNC_OBJECT_TYPE: "Content", name: "Top Unit 1", lastUpdatedOn: "2017-03-06T15:49:23.343+0000", contentType: "TextBookUnit", IL_UNIQUE_ID: "do_112196375456505856121", status: "Draft"}) return n""",
			"""MATCH (n: domain{IL_UNIQUE_ID:'do_212198568993210368181'}), (c: domain{IL_UNIQUE_ID:'do_112196373243092992119'}) CREATE (n)-[r:hasSequenceMember]->(c)""",
			"""MATCH (n: domain{IL_UNIQUE_ID:'do_212198568993210368181'}), (c: domain{IL_UNIQUE_ID:'do_112196373711413248120'}) CREATE (n)-[r:hasSequenceMember]->(c)""",
			"""MATCH (n: domain{IL_UNIQUE_ID:'do_21219907978217062412'}), (c: domain{IL_UNIQUE_ID:'do_112196373711413248120'}) CREATE (n)-[r:hasSequenceMember]->(c)""",
			"""MATCH (n: domain{IL_UNIQUE_ID:'do_21219907978217062412'}), (c: domain{IL_UNIQUE_ID:'do_112196375456505856121'}) CREATE (n)-[r:hasSequenceMember]->(c)""");
		executeQueries(queries);
	}
	
	private def linkBookUnitsWithContents() {
		val queries = List("""MATCH (n: domain{IL_UNIQUE_ID:'do_112196373243092992119'}), (c: domain{IL_UNIQUE_ID:'org.ekstep.ra_ms_52d058e969702d5fe1ae0f00'}) CREATE (n)-[r:hasSequenceMember]->(c)""",
			"""MATCH (n: domain{IL_UNIQUE_ID:'do_112196373711413248120'}), (c: domain{IL_UNIQUE_ID:'org.ekstep.ra_ms_52d02eae69702d0905cf0800'}) CREATE (n)-[r:hasSequenceMember]->(c)""",
			"""MATCH (n: domain{IL_UNIQUE_ID:'do_112196375456505856121'}), (c: domain{IL_UNIQUE_ID:'org.ekstep.ra_ms_5391b1d669702d107e030000'}) CREATE (n)-[r:hasSequenceMember]->(c)""",
			"""MATCH (n: domain{IL_UNIQUE_ID:'do_112196375456505856121'}), (c: domain{IL_UNIQUE_ID:'org.ekstep.ra_ms_52d058e969702d5fe1ae0f00'}) CREATE (n)-[r:hasSequenceMember]->(c)""");
		executeQueries(queries);
	}
	
	private def updateAppIdChannel() {
	    val queries = List("MATCH (n:domain) SET n.appId='genie', n.channel='in.ekstep'")
	    executeQueries(queries);
	}

}