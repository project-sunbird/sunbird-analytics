package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.CommonUtil
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.json4s.jvalue2extractable
import org.json4s.string2JsonInput
import org.scalatest.BeforeAndAfterAll
import com.fasterxml.jackson.core.JsonParseException
import org.ekstep.analytics.framework.util.JSONUtils
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.conf.AppConf

/**
 * @author Santhosh
 */
class SparkSpec(val file: String = "src/test/resources/sample_telemetry.log") extends BaseSpec with BeforeAndAfterAll {

    var events: RDD[Event] = null;
    implicit var sc: SparkContext = null;

    override def beforeAll() {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        sc = CommonUtil.getSparkContext(1, "TestAnalyticsCore");
        events = loadFile[Event](file)
        val connector = CassandraConnector(sc.getConf);
        val session = connector.openSession();
        val dataLoader = new CQLDataLoader(session);
        dataLoader.load(new FileCQLDataSet(AppConf.getConfig("cassandra.cql_path"), true, true));
    }

    override def afterAll() {
        CommonUtil.closeSparkContext();
    }

    def loadFile[T](file: String)(implicit mf: Manifest[T]): RDD[T] = {
        if (file == null) {
            return null;
        }
        sc.textFile(file, 1).map { line => JSONUtils.deserialize[T](line) }.filter { x => x != null }.cache();
    }

}