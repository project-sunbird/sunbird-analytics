package org.ekstep.analytics.api.service.experiment

import scala.collection.mutable

trait ExperimentTypeResolver {
  def getType: String
  def resolve(data: ExperimentData): Boolean
}

object ExperimentResolver {

  val resolverMap: mutable.Map[String, ExperimentTypeResolver] = mutable.Map()

  def register(resolver: ExperimentTypeResolver)  = {
    resolverMap += (resolver.getType -> resolver)
  }

  def getResolver(expType: String): Option[ExperimentTypeResolver] = {
    if(resolverMap.contains(expType)) resolverMap.get(expType) else None
  }
}
