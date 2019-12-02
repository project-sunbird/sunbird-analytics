package org.ekstep.analytics.util

import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic
import pl.allegro.tech.embeddedelasticsearch.PopularProperties.{ CLUSTER_NAME, HTTP_PORT, TRANSPORT_TCP_PORT }
import pl.allegro.tech.embeddedelasticsearch.IndexSettings
import scala.collection.mutable.Buffer
import scala.collection.JavaConverters._
import pl.allegro.tech.embeddedelasticsearch.IndexRequest
import scala.collection.JavaConverters
import java.util.concurrent.TimeUnit.MINUTES

case class EsIndex(index: String, indexType: Option[String], mappingSettings: Option[String], aliasSettings: Option[String])
object EmbeddedES {

  var esServer: EmbeddedElastic = null;

  def start(indices: Array[EsIndex]) {
    val builder = EmbeddedElastic.builder()
      .withElasticVersion("6.3.0")
      .withSetting(HTTP_PORT, "9200")
      .withSetting(CLUSTER_NAME, "TestCluster")
      .withEsJavaOpts("-Xms128m -Xmx512m")
     .withStartTimeout(1,MINUTES);

    indices.foreach(f => {
      val indexSettingsBuilder = IndexSettings.builder();
      if (f.mappingSettings.nonEmpty) indexSettingsBuilder.withType(f.indexType.get, f.mappingSettings.get)
      if (f.aliasSettings.nonEmpty) indexSettingsBuilder.withAliases(f.aliasSettings.get)
      builder.withIndex(f.index, indexSettingsBuilder.build())
    })
    esServer = builder.build().start();
  }

  def loadData(indexName: String, indexType: String, indexData: Buffer[String]) = {
    val docs = indexData.map(f => {
      new IndexRequest.IndexRequestBuilder(indexName, indexType, f).build()
    })
    esServer.index(JavaConverters.bufferAsJavaListConverter(docs).asJava);
  }

  def getAllDocuments(index: String): Buffer[String] = {
    esServer.fetchAllDocuments(index).asScala;
  }
  
  def stop() {
    if(esServer != null)
      esServer.stop();
  }
}