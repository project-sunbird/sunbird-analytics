package org.ekstep.ilimi.analytics.framework.util

import scala.concurrent._
import scala.concurrent.duration._
import java.net.URL
import org.ekstep.ilimi.analytics.framework.Response
import org.ekstep.ilimi.analytics.framework.Metadata
import org.ekstep.ilimi.analytics.framework.Search
import org.ekstep.ilimi.analytics.framework.Request
import org.ekstep.ilimi.analytics.framework.SearchFilter
import java.nio.charset.Charset
import ExecutionContext.Implicits.global
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.methods.HttpGet
import scala.io.Source
import org.apache.http.client.methods.HttpPost
import org.apache.http.HttpEntity
import org.apache.http.entity.StringEntity

/**
 * @author Santhosh
 */
object RestUtil {

    def main(args: Array[String]): Unit = {
        val url = Constants.getContentAPIUrl("org.ekstep.story.hi.elephant");
        val response = RestUtil.get[Response](url);
        Console.println("response", response);
    }

    def get[T](apiURL: String)(implicit mf: Manifest[T]) = {
        val httpClient = HttpClients.createDefault();
        val request = new HttpGet(apiURL);
        request.addHeader("user-id", "analytics");
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity()
            val content = if (entity != null) {
                val inputStream = entity.getContent()
                val contentStr = Source.fromInputStream(inputStream).getLines.mkString
                inputStream.close
                contentStr;
            } else null;
            JSONUtils.deserialize[T](content);
        } finally {
            httpClient.close()
        }
    }

    def post[T](apiURL: String, body: String)(implicit mf: Manifest[T]) = {
        
        val httpClient = HttpClients.createDefault();
        val request = new HttpPost(apiURL);
        request.addHeader("user-id", "analytics");
        request.addHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity(body));
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity()
            val content = if (entity != null) {
                val inputStream = entity.getContent()
                val contentStr = Source.fromInputStream(inputStream).getLines.mkString
                inputStream.close
                contentStr;
            } else null;
            JSONUtils.deserialize[T](content);
        } finally {
            httpClient.close()
        }
    }

    private def getRestContent(url: String): String = {
        val httpClient = HttpClients.createDefault();
        val request = new HttpGet(url);
        request.addHeader("user-id", "analytics");
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity()
            if (entity != null) {
                val inputStream = entity.getContent()
                val contentStr = Source.fromInputStream(inputStream).getLines.mkString
                inputStream.close
                contentStr;
            } else null;
        } finally {
            httpClient.close()
        }
    }
}