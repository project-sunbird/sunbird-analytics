package org.ekstep.analytics.api.service.experiment.Resolver

import org.ekstep.analytics.api.service.experiment.{ExperimentData, ExperimentTypeResolver}

import scala.util.hashing.MurmurHash3

class ModulusResolver extends ExperimentTypeResolver {

  override def getType: String = "modulus"

  def checkModulus(key: String, divisor: Long) = {
    val hash = MurmurHash3.stringHash(key)
    divisor > 0 && (hash % divisor) == 0
  }

  override def resolve(data: ExperimentData): Boolean = {
    if (data.userId != null && data.userId.nonEmpty) {
      checkModulus(data.userId, data.userIdMod)
    } else if(data.deviceId != null && data.deviceId.nonEmpty) {
      checkModulus(data.deviceId, data.deviceIdMod)
    } else {
      false
    }
  }
}


