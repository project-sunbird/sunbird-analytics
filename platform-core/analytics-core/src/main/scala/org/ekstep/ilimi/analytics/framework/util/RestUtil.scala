package org.ekstep.ilimi.analytics.framework.util

import com.stackmob.newman._
import com.stackmob.newman.serialization._
import com.stackmob.newman.dsl._
import scala.concurrent._
import scala.concurrent.duration._
import java.net.URL
import com.stackmob.newman.ApacheHttpClient
import org.ekstep.ilimi.analytics.framework.Response
import org.ekstep.ilimi.analytics.framework.Metadata
import org.ekstep.ilimi.analytics.framework.Search
import org.ekstep.ilimi.analytics.framework.Request
import org.ekstep.ilimi.analytics.framework.SearchFilter
import java.nio.charset.Charset

/**
 * @author Santhosh
 */
object RestUtil {

    def get[T](apiURL: String) (implicit mf:Manifest[T]) = {
        implicit val httpClient = new ApacheHttpClient;
        val url = new URL(apiURL);
        val get = GET(url).addHeaders("user-id" -> "analytics");
        val response = Await.result(get.apply, 10.second);
        JSONUtils.deserialize[T](response.bodyString);
    }
    
    def post[T](apiURL: String, body: String) (implicit mf:Manifest[T]) = {
        implicit val httpClient = new ApacheHttpClient;
        val url = new URL(apiURL);
        val post = POST(url).addHeaders("user-id" -> "ilimi", "Content-Type" -> "application/json").addBody(body);
        val response = Await.result(post.apply, 10.second);
        JSONUtils.deserialize[T](response.bodyString);
    }
}