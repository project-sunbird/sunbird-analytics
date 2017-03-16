package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.spark.rdd.RDD
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.logging.log4j.Logger
import org.ekstep.analytics.framework.JobContext
import org.scalatest.BeforeAndAfterAll

import com.datastax.spark.connector.cql.CassandraConnector

/**
 * @author Santhosh
 */
class SparkSpec(val file: String = "src/test/resources/sample_telemetry.log") extends BaseSpec with BeforeAndAfterAll {

    var events: RDD[Event] = null;
    implicit var sc: SparkContext = null;

    override def beforeAll() {
        JobLogger.init("org.ekstep.analytics.test-cases");
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        sc = CommonUtil.getSparkContext(1, "TestAnalyticsCore");
        events = loadFile[Event](file)
        val connector = CassandraConnector(sc.getConf);
        val session = connector.openSession();
        val dataLoader = new CQLDataLoader(session);
        dataLoader.load(new FileCQLDataSet(AppConf.getConfig("cassandra.cql_path"), true, true));
    }

    override def afterAll() {
        JobContext.cleanUpRDDs();
        CommonUtil.closeSparkContext();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra()
    }

    def loadFile[T](file: String)(implicit mf: Manifest[T]): RDD[T] = {
        if (file == null) {
            return null;
        }
        sc.textFile(file, 1).map { line => JSONUtils.deserialize[T](line) }.filter { x => x != null }.cache();
    }
}