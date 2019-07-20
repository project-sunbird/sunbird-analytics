package org.ekstep.analytics.api.service.experiment.Resolver

import org.ekstep.analytics.api.service.experiment.{ExperimentData, ExperimentTypeResolver}

import scala.util.hashing.MurmurHash3

class ModulusResolver extends ExperimentTypeResolver {

  override def getType: String = "modulus"

  def checkHash(key: String, divisor: Long) = {
    val hash = MurmurHash3.stringHash(key)
    if (divisor > 0 && (hash % divisor) == 0) true else false
  }

  override def resolve(data: ExperimentData): Boolean = {
    if (data.userId != null && data.userId.nonEmpty) {
      checkHash(data.userId, data.userIdMod)
    } else if(data.deviceId != null && data.deviceId.nonEmpty) {
      checkHash(data.deviceId, data.deviceIdMod)
    } else {
      false
    }
  }
}


