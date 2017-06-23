package org.ekstep.analytics.api

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.ekstep.analytics.framework.conf.AppConf
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.datastax.spark.connector.cql.CassandraConnector
import org.scalatest.BeforeAndAfterAll

/**
 * @author Santhosh
 */
class BaseSpec extends FlatSpec with Matchers with BeforeAndAfterAll{
  
  override def beforeAll() {
    if (embeddedCassandraMode) {
      System.setProperty("cassandra.unsafesystem", "true")
		  EmbeddedCassandraServerHelper.startEmbeddedCassandra(20000L);
		  val connector = CassandraConnector(getSparkConf());
		  val session = connector.openSession();
		  val dataLoader = new CQLDataLoader(session);
		  dataLoader.load(new FileCQLDataSet(AppConf.getConfig("cassandra.cql_path"), true, true));
    }
	}

	override def afterAll() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra()
	}
	
	private def getSparkConf(): SparkConf = {
		val conf = new SparkConf().setAppName("TestAnalyticsCore");
		conf.setMaster("local[*]");
		if (!conf.contains("spark.cassandra.connection.host"))
			conf.set("spark.cassandra.connection.host", AppConf.getConfig("spark.cassandra.connection.host"))
		if (embeddedCassandraMode)
			conf.set("spark.cassandra.connection.port", AppConf.getConfig("cassandra.service.embedded.connection.port"))
		if (!conf.contains("reactiveinflux.url")) {
			conf.set("reactiveinflux.url", AppConf.getConfig("reactiveinflux.url"));
		}
		conf;
	}

	private def embeddedCassandraMode(): Boolean = {
		val isEmbedded = AppConf.getConfig("cassandra.service.embedded.enable");
		StringUtils.isNotBlank(isEmbedded) && StringUtils.equalsIgnoreCase("true", isEmbedded);
	}

}