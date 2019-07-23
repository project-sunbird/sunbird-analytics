package org.ekstep.analytics.api.service.experiment

import scala.collection.mutable

trait ExperimentTypeResolver {
  def getType: String = "root"
  def resolve(data: ExperimentData): Boolean = true
}

object ExperimentResolver {

  val resolverMap: mutable.Map[String, ExperimentTypeResolver] = mutable.Map()

  def register(resolver: ExperimentTypeResolver)  = {
    resolverMap += (resolver.getType -> resolver)
  }

  def getResolver(expType: String): ExperimentTypeResolver = {
      resolverMap.getOrElse(expType, new ExperimentTypeResolver{})
  }
}
