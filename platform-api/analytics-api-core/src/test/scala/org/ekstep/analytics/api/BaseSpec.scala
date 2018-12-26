package org.ekstep.analytics.api

import org.apache.commons.lang3.StringUtils
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.ekstep.analytics.api.util.DBUtil
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import com.typesafe.config.ConfigFactory
import org.scalatest.mock.MockitoSugar

/**
 * @author Santhosh
 */
class BaseSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {
  implicit val config = ConfigFactory.load()

  override def beforeAll() {
    if (embeddedCassandraMode) {
      System.setProperty("cassandra.unsafesystem", "true")
		  EmbeddedCassandraServerHelper.startEmbeddedCassandra(20000L);
		  val session = DBUtil.session;
		  val dataLoader = new CQLDataLoader(session);
		  dataLoader.load(new FileCQLDataSet(AppConf.getConfig("cassandra.cql_path"), true, true));
    }
	}

	override def afterAll() {
		if (embeddedCassandraMode) {
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
			EmbeddedCassandraServerHelper.stopEmbeddedCassandra()
		}
	}

	private def embeddedCassandraMode(): Boolean = {
		val isEmbedded = AppConf.getConfig("cassandra.service.embedded.enable");
		StringUtils.isNotBlank(isEmbedded) && StringUtils.equalsIgnoreCase("true", isEmbedded);
	}
	
	def loadFileData[T](file: String)(implicit mf: Manifest[T]): Array[T] = {
        if (file == null) {
          return null;
        }
        scala.io.Source.fromFile(file).getLines().toList.map(line => JSONUtils.deserialize[T](line)).filter { x => x != null }.toArray
    }

}