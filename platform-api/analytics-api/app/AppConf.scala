package appconf

import akka.actor.{ActorRef, Props}
import com.typesafe.config.{Config, ConfigFactory}
import org.ekstep.analytics.api.service.DeviceRegisterService
import org.ekstep.analytics.api.util.{CommonUtil, JSONUtils}

import scala.collection.JavaConverters.mapAsJavaMapConverter

object AppConf {

    var actors = scala.collection.mutable.Map[String, ActorRef]()


  val config: Config = play.Play.application.configuration.underlying()
    .withFallback(ConfigFactory.parseMap(Map("content2vec.scripts_path" -> "",
      "python.home" -> "",
      "content2vec.download_path" -> "/tmp",
      "content2vec.download_file_prefix" -> "temp_",
      "content2vec.enrich_content" -> "true",
      "content2vec.content_corpus" -> "true",
      "content2vec.train_model" -> "false",
      "content2vec.s3_bucket" -> "ekstep-dev-data-store",
      "content2vec.model_path" -> "model",
      "content2vec.s3_key_prefix" -> "model",
      "content2vec.infer_all" -> "false",
      "content2vec.corpus_path" -> "").asJava))

    def setActorRef(name: String, actor: ActorRef): Unit = {
      actors.put(name, actor)
    }

    def getActorRef(name: String): ActorRef = {
        actors(name)
    }

    val successResponse = JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
        Map("message" -> s"Device registered successfully")))
}
