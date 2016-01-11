package org.ekstep.analytics.framework.util

import scala.io.Source

import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.ekstep.analytics.framework.Response

/**
 * @author Santhosh
 */
object RestUtil {

    def get[T](apiURL: String)(implicit mf: Manifest[T]) = {
        val httpClient = HttpClients.createDefault();
        val request = new HttpGet(apiURL);
        request.addHeader("user-id", "analytics");
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity()
            var content:String = null;
            if (entity != null) {
                val inputStream = entity.getContent()
                content = Source.fromInputStream(inputStream, "UTF-8").getLines.mkString;
                inputStream.close
            }
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
            var content:String = null;
            if (entity != null) {
                val inputStream = entity.getContent()
                content = Source.fromInputStream(inputStream).getLines.mkString
                inputStream.close
            }
            JSONUtils.deserialize[T](content);
        } finally {
            httpClient.close()
        }
    }

}