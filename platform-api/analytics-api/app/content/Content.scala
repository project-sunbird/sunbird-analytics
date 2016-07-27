package content

import play.api._
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.api.Response

object Content {
  
  implicit val config = Map(
          "base.url" -> play.Play.application.configuration.getString("base.url"),
          "python.scripts.loc" -> play.Play.application.configuration.getString("python.scripts.loc")
      );
  
  def setContent()(implicit sc: SparkContext) = {
    val baseUrl = config.get("base.url").get;
    var searchUrl = s"$baseUrl/search/v2/search";
    println("BaseUrl: "+searchUrl);
    val request = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Live")), "limit" -> 1000));
    println("Request: "+JSONUtils.serialize(request));
    var resp = RestUtil.post[Response](searchUrl, JSONUtils.serialize(request));
    println("Result: "+JSONUtils.serialize(resp));
//    var content = resp.result.toSeq;
//    var contentRDD = sc.parallelize(content);
//    contentRDD.persist();
  } 
}