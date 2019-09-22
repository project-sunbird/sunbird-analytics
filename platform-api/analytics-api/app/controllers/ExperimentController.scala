package controllers

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import javax.inject.{Inject, Named}
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.service.ExperimentAPIService.{CreateExperimentRequest, _}
import org.ekstep.analytics.api.util.JSONUtils
import play.api.Configuration
import play.api.libs.json.Json
import play.api.mvc.{Request, _}

import scala.concurrent.ExecutionContext


class ExperimentController @Inject() (
 @Named("experiment-actor") experimentActor: ActorRef,
 system: ActorSystem,
 configuration: Configuration,
 cc: ControllerComponents
) (implicit ec: ExecutionContext)
  extends BaseController(cc, configuration) {

    def createExperiment() = Action.async { request: Request[AnyContent] =>
        val body: String = Json.stringify(request.body.asJson.get)
        val res = ask(experimentActor, CreateExperimentRequest(body, config)).mapTo[ExperimentBodyResponse]
        res.map { x =>
            result(x.responseCode, JSONUtils.serialize(x))
        }
    }

    def getExperiment(experimentId: String) = Action.async { request: Request[AnyContent] =>
        val res = ask(experimentActor, GetExperimentRequest(experimentId, config)).mapTo[Response]
        res.map { x =>
            result(x.responseCode, JSONUtils.serialize(x))
        }
    }

}