package org.ekstep.analytics.model

import org.scalatest._
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.spark.SparkConf
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.commons.lang3.StringUtils
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic
import pl.allegro.tech.embeddedelasticsearch.PopularProperties.{ CLUSTER_NAME, HTTP_PORT, TRANSPORT_TCP_PORT }
import pl.allegro.tech.embeddedelasticsearch.IndexSettings
import pl.allegro.tech.embeddedelasticsearch.IndexSettings.Builder
import java.util.concurrent.TimeUnit.MINUTES
import org.joda.time.DateTimeUtils
import org.joda.time.DateTime
import org.apache.spark.SparkContext
import org.ekstep.analytics.util.EmbeddedCassandra
import org.apache.spark.sql.SparkSession

/**
 * @author Santhosh
 */
class BaseSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  def getSparkContext(): SparkContext = {
    getSparkSession().sparkContext;
  }

  def getSparkConf(): SparkConf = {
    val conf = new SparkConf().setAppName("AnalyticsTestSuite").set("spark.default.parallelism", "2");
    conf.set("spark.sql.shuffle.partitions", "2")
    conf.setMaster("local[*]")
    conf.set("spark.driver.memory", "1g")
    conf.set("spark.memory.fraction", "0.3")
    conf.set("spark.memory.storageFraction", "0.5")
    conf.set("spark.cassandra.connection.port", AppConf.getConfig("cassandra.service.embedded.connection.port"))
    conf.set("es.nodes", "http://localhost")
    conf;
  }

  override def beforeAll() {

    EmbeddedCassandra.setup();
  }

  override def afterAll() {
    EmbeddedCassandra.close();
  }
  
  def getSparkSession() : SparkSession = {
    SparkSession.builder.config(getSparkConf()).getOrCreate()
  }

}