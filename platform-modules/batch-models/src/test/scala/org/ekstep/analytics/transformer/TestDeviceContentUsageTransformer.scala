package org.ekstep.analytics.transformer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.model.DeviceContentSummary
import org.ekstep.analytics.model.DeviceContentUsageSummaryModel
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.Event

class TestDeviceContentUsageTransformer extends SparkSpec(null) {

    "DeviceContentUsageTransformer" should "perform binning and outlier on DCUS" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE device_db.device_content_summary_fact");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, app_id, channel_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'domain_68601','Genie','Ekstep', null, 1459641600, false, null, 1461715199, 0, 0, 1, 1, 1459641600, 10, 10);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, app_id, channel_id, avg_interactions_min, download_date, downloaded, game_ver, last_played_on, mean_play_time_interval, num_group_user, num_individual_user, num_sessions, start_time, total_interactions, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a036', 'domain_63844','Genie','Ekstep', null, 1459641600, true, null, 1461715199, 1, 10, 20, 100, 1459641600, 1534, 1234);");
            session.execute("INSERT INTO device_db.device_content_summary_fact(device_id, content_id, app_id, channel_id, avg_interactions_min, downloaded, game_ver, mean_play_time_interval, total_timespent) VALUES ('9ea6702483ff7d4fcf9cb886d0ff0e1ebc25a034', 'domain_63840','Genie','Ekstep', null, true, null, 1, 1234);");
            
        }

        val table = sc.cassandraTable[DeviceContentSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT)
        val out = DeviceContentUsageTransformer.getTransformationByBinning(table, 4)
        out.count() should be (table.count())
    }

}