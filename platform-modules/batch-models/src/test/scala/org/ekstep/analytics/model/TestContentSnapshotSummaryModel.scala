/**
 * @author Sowmya Dixit
 */
package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.apache.commons.lang3.StringUtils
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._

class TestContentSnapshotSummaryModel extends SparkGraphSpec(null) {

    "ContentSnapshotSummaryModel" should "generate content snapshot summary events" in {

        // Running all VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));

        // Populate ce usage summary fact table
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE creation_metrics_db.ce_usage_summary_fact");
            session.execute("INSERT INTO creation_metrics_db.ce_usage_summary_fact(d_period, d_content_id, unique_users_count, total_sessions, total_ts, avg_ts_session, updated_date) VALUES (0, 'org.ekstep.ra_ms_52d058e969702d5fe1ae0f00', 0, 0, 20.0, 20.0, 1475731808000);");
            session.execute("INSERT INTO creation_metrics_db.ce_usage_summary_fact(d_period, d_content_id, unique_users_count, total_sessions, total_ts, avg_ts_session, updated_date) VALUES (0, 'org.ekstep.ra_ms_52d02eae69702d0905cf0800', 0, 0, 0.0, 20.0, 1475731808000);");
            session.execute("INSERT INTO creation_metrics_db.ce_usage_summary_fact(d_period, d_content_id, unique_users_count, total_sessions, total_ts, avg_ts_session, updated_date) VALUES (0, 'org.ekstep.ra_ms_5391b1d669702d107e030000', 0, 0, 20.0, 20.0, 1475731808000);");
            session.execute("INSERT INTO creation_metrics_db.ce_usage_summary_fact(d_period, d_content_id, unique_users_count, total_sessions, total_ts, avg_ts_session, updated_date) VALUES (0, 'test_content', 0, 0, 20.0, 20.0, 1475731808000);");
        }

        val rdd = ContentSnapshotSummaryModel.execute(sc.makeRDD(List()), None);
        val events = rdd.collect

        events.length should be(9)

        val event1 = rdd.filter { x => StringUtils.equals(x.dimensions.author_id.get, "all") && StringUtils.equals(x.dimensions.partner_id.get, "all") }.first();

        // Check for author_id = all
        event1.context.pdata.model should be("ContentSnapshotSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SNAPSHOT");
        event1.context.date_range should not be null;
        event1.dimensions.author_id.get should be("all")
        event1.dimensions.partner_id.get should be("all")

        val eks1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks1.get("live_content_count").get should be(1)
        eks1.get("total_content_count").get should be(4)
        eks1.get("review_content_count").get should be(0)
        eks1.get("total_user_count").get should be(2)
        eks1.get("active_user_count").get should be(0)
        eks1.get("creation_ts").get should be(60.0)
        eks1.get("avg_creation_ts").get should be(20.0)

        val event2 = rdd.filter { x => StringUtils.equals(x.dimensions.author_id.get, "290") && StringUtils.equals(x.dimensions.partner_id.get, "all") }.first();

        // Check for specific author_id
        event2.context.pdata.model should be("ContentSnapshotSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("SNAPSHOT");
        event2.context.date_range should not be null;
        event2.dimensions.author_id.get should be("290")
        event2.dimensions.partner_id.get should be("all")

        val eks2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks2.get("live_content_count").get should be(1)
        eks2.get("total_content_count").get should be(3)
        eks2.get("review_content_count").get should be(0)
        eks2.get("total_user_count").get should be(0)
        eks2.get("active_user_count").get should be(0)
        eks2.get("creation_ts").get should be(40.0)
        eks2.get("avg_creation_ts").get should be(20.0)

        val event3 = rdd.filter { x => StringUtils.equals(x.dimensions.author_id.get, "291") && StringUtils.equals(x.dimensions.partner_id.get, "all") }.first();

        event3.context.pdata.model should be("ContentSnapshotSummarizer");
        event3.context.pdata.ver should be("1.0");
        event3.context.granularity should be("SNAPSHOT");
        event3.context.date_range should not be null;
        event3.dimensions.author_id.get should be("291")
        event3.dimensions.partner_id.get should be("all")

        val eks3 = event3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks3.get("live_content_count").get should be(0)
        eks3.get("total_content_count").get should be(1)
        eks3.get("review_content_count").get should be(0)
        eks3.get("total_user_count").get should be(0)
        eks3.get("active_user_count").get should be(0)
        eks3.get("creation_ts").get should be(20.0)
        eks3.get("avg_creation_ts").get should be(20.0)

        // check for specific partner_id
        val event4 = rdd.filter { x => StringUtils.equals(x.dimensions.author_id.get, "all") && StringUtils.equals(x.dimensions.partner_id.get, "org.ekstep.partner.pratham") }.first();

        event4.context.pdata.model should be("ContentSnapshotSummarizer");
        event4.context.pdata.ver should be("1.0");
        event4.context.granularity should be("SNAPSHOT");
        event4.context.date_range should not be null;
        event4.dimensions.author_id.get should be("all")
        event4.dimensions.partner_id.get should be("org.ekstep.partner.pratham")

        val eks4 = event4.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks4.get("live_content_count").get should be(1)
        eks4.get("total_content_count").get should be(3)
        eks4.get("review_content_count").get should be(0)
        eks4.get("total_user_count").get should be(1)
        eks4.get("active_user_count").get should be(0)
        eks4.get("creation_ts").get should be(40.0)
        eks4.get("avg_creation_ts").get should be(20.0)

        // check for partner_id & author_id combination
        val event5 = rdd.filter { x => StringUtils.equals(x.dimensions.author_id.get, "290") && StringUtils.equals(x.dimensions.partner_id.get, "org.ekstep.partner.pratham") }.first();

        event5.context.pdata.model should be("ContentSnapshotSummarizer");
        event5.context.pdata.ver should be("1.0");
        event5.context.granularity should be("SNAPSHOT");
        event5.context.date_range should not be null;
        event5.dimensions.author_id.get should be("290")
        event5.dimensions.partner_id.get should be("org.ekstep.partner.pratham")

        val eks5 = event5.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks5.get("live_content_count").get should be(1)
        eks5.get("total_content_count").get should be(3)
        eks5.get("review_content_count").get should be(0)
        eks5.get("total_user_count").get should be(0)
        eks5.get("active_user_count").get should be(0)
        eks5.get("creation_ts").get should be(40.0)
        eks5.get("avg_creation_ts").get should be(20.0)

    }

    it should "generate content snapshot summary event for active user count > 0" in {

        // Running all VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE creation_metrics_db.ce_usage_summary_fact");
        }

        val createdOn = new DateTime().toString()
        val query1 = s"CREATE (usr:User {type:'author', IL_UNIQUE_ID:'test_author', contentCount:0, liveContentCount:0})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content', IL_UNIQUE_ID:'test_content', contentType:'story', createdFor:['org.ekstep.partner1'], createdOn:'$createdOn'}) RETURN usr.IL_UNIQUE_ID, cnt.createdOn"
        GraphQueryDispatcher.dispatch(query1)

        val rdd = ContentSnapshotSummaryModel.execute(sc.makeRDD(List()), None);
        val events = rdd.collect

        events.length should be(11)

        // Check for author_id = all
        val event1 = events(0);

        event1.context.pdata.model should be("ContentSnapshotSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("SNAPSHOT");
        event1.context.date_range should not be null;
        event1.dimensions.author_id.get should be("all")
        event1.dimensions.partner_id.get should be("all")

        val eks1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks1.get("live_content_count").get should be(1)
        eks1.get("total_content_count").get should be(5)
        eks1.get("review_content_count").get should be(0)
        eks1.get("total_user_count").get should be(3)
        eks1.get("active_user_count").get should be(1)
        eks1.get("creation_ts").get should be(0.0)
        eks1.get("avg_creation_ts").get should be(0.0)

        // Check for specific partner_id
        val event2 = events(5);

        event2.context.pdata.model should be("ContentSnapshotSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("SNAPSHOT");
        event2.context.date_range should not be null;
        event2.dimensions.author_id.get should be("all")
        event2.dimensions.partner_id.get should be("org.ekstep.partner1")

        val eks2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks2.get("live_content_count").get should be(1)
        eks2.get("total_content_count").get should be(3)
        eks2.get("review_content_count").get should be(0)
        eks2.get("total_user_count").get should be(1)
        eks2.get("active_user_count").get should be(1)
        eks2.get("creation_ts").get should be(0.0)
        eks2.get("avg_creation_ts").get should be(0.0)

    }

    it should "check content snapshot summary event for empty partner_id" in {

        // Running all VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));

        val createdOn = new DateTime().toString()
        val query1 = s"CREATE (usr:User {type:'author', IL_UNIQUE_ID:'test_author_1', contentCount:0, liveContentCount:0})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content', IL_UNIQUE_ID:'test_content_1', contentType:'story', createdFor:[''], createdOn:'$createdOn'}) RETURN usr.IL_UNIQUE_ID, cnt.createdOn, cnt.createdFor"
        val res = GraphQueryDispatcher.dispatch(query1)

        val rdd = ContentSnapshotSummaryModel.execute(sc.makeRDD(List()), None);
        val events = rdd.collect

        events.length should be(10)

        val out = rdd.filter { x => StringUtils.isBlank(x.dimensions.partner_id.get) }
        out.count() should be(0)

    }
}