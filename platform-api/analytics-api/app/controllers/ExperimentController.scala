package controllers

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.routing.FromConfig
import javax.inject.{Inject, Singleton}
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.service.{CreateExperimentRequest, ExperimentAPIService, GetExperimentRequest}
import org.ekstep.analytics.api.util.JSONUtils
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.Json
import play.api.mvc._



@Singleton
class ExperimentController @Inject()(system: ActorSystem) extends BaseController {
    val expApiActor = system.actorOf(Props[ExperimentAPIService].withRouter(FromConfig()), name = "expApiActor")

    def createExperiment() = Action.async { implicit request =>
        val body: String = Json.stringify(request.body.asJson.get)
        val res = ask(expApiActor, CreateExperimentRequest(body, config)).mapTo[ExperimentBodyResponse]
        res.map { x =>
            result(x.responseCode, JSONUtils.serialize(x))
        }
    }

    def getExperiment(experimentId: String) = Action.async { implicit request =>

        val res = ask(expApiActor, GetExperimentRequest(experimentId, config)).mapTo[Response]
        res.map { x =>
            result(x.responseCode, JSONUtils.serialize(x))
        }
    }

}