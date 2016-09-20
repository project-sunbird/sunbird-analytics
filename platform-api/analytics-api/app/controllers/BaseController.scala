package controllers

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.duration.DurationInt

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.util.Timeout
import akka.util.Timeout.durationToTimeout
import play.api.mvc.Controller

abstract class BaseController extends Controller {
	implicit val timeout: Timeout = 20 seconds;
	implicit val config: Config = play.Play.application.configuration.underlying()
		.withFallback(ConfigFactory.parseMap(Map("content2vec.scripts_path" -> "",
			"python.home" -> "",
			"content2vec.download_path" -> "/tmp",
			"content2vec.download_file_prefix" -> "temp_",
			"content2vec.enrich_content" -> "true",
			"content2vec.content_corpus" -> "true",
			"content2vec.train_model" -> "false",
			"content2vec.s3_bucket" -> "sandbox-data-store",
			"content2vec.model_path" -> "model",
			"content2vec.s3_key_prefix" -> "model",
			"content2vec.infer_all" -> "false",
			"content2vec.corpus_path" -> "").asJava));
}