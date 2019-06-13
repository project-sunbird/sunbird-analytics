package org.ekstep.analytics.api.service

import akka.actor.{Actor, ActorRef}
import org.apache.logging.log4j.LogManager
import org.ekstep.analytics.api.util.JSONUtils

case class Dspec (os: String = "", make: String = "", mem: Int = 0, idisk: String = "", edisk: String = "", scrn: String = "", camera: String = "", cpu: String = "", sims: Int = 0, uaspec: Uaspec)
case class Uaspec (agent: String = "", ver: String = "", system: String = "", platform: String = "", raw: String = "")
case class ValidatorMessage(status: Boolean, msg: String)

class validator() {
  def isNullOrEmpty(str: String): Boolean = if (str != null && ! str.isEmpty) false else true
}

case class Context (pdata: Pdata, did: String) extends validator {
  def validate: ValidatorMessage = {
    if (pdata == null) {
      ValidatorMessage(false, "property: pdata is missing!")
    } else if (isNullOrEmpty(did)) {
      ValidatorMessage(false, "property: did is null or empty!")
    } else {
      ValidatorMessage(true, "")
    }
  }
}

case class Edata (dspec: Option[Dspec], crashts: String, crash_logs: String) extends validator {
  def validate: ValidatorMessage = {
    if (isNullOrEmpty(crashts)) {
      ValidatorMessage(false, "property: crashts is null or empty!")
    } else if (isNullOrEmpty(crash_logs)) {
      ValidatorMessage(false, "property: crash_logs is null or empty!")
    } else {
      ValidatorMessage(true, "")
    }
  }
}

case class Pdata (id: String, ver: String, pid: String) extends validator {
  def validate: ValidatorMessage = {
    if (isNullOrEmpty(id)) {
      ValidatorMessage(false, "property: pdata.id is null or empty!")
    } else if (isNullOrEmpty(pid)) {
      ValidatorMessage(false, "property: pdata.pid is null or empty!")
    } else if (isNullOrEmpty(ver)) {
      ValidatorMessage(false, "property: pdata.ver is null or empty!")
    } else {
      ValidatorMessage(true, "")
    }
  }
}

case class ClientRequestBody (context: Context, edata: Edata) extends validator {
  def validate: ValidatorMessage = {
    if (context == null) {
      ValidatorMessage(false, "property: context is missing!")
    } else if (edata == null) {
      ValidatorMessage(false, "property: edata is missing!")
    } else {
      ValidatorMessage(true, "")
    }
  }
}


case class ClientLogRequest(request: Option[ClientRequestBody]) extends validator {
  def validate: ValidatorMessage = {
    request match {
      case None => ValidatorMessage(false, "property: request is missing!")
      case Some(requestObj) => if (!requestObj.validate.status) {
        ValidatorMessage(false, requestObj.validate.msg)
      } else if (!requestObj.context.validate.status) {
        ValidatorMessage(false,  requestObj.context.validate.msg)
      } else if (!requestObj.context.pdata.validate.status) {
        ValidatorMessage(false,  requestObj.context.pdata.validate.msg)
      } else if (!requestObj.edata.validate.status) {
        ValidatorMessage(false,  requestObj.edata.validate.msg)
      } else {
        ValidatorMessage(true, "")
      }
    }
  }
}

class ClientLogsAPIService extends Actor {
  private val logger = LogManager.getLogger("crash-logger")
  override def receive: Receive = {
    case ClientLogRequest(request: Option[ClientRequestBody]) => {
      request match {
        case Some(log) => logger.info(JSONUtils.serialize(log))
      }
    }
  }
}
