package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.job.IGraphExecutionModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.Job_Config
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import collection.JavaConversions._
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._

case class Request(grade_level: List[String], concepts: List[String], content_type: String, language: Map[String, String], `type`: String)
case class RequestRecos(uid: String, requests: List[Request])

object CreationRecommendationModel extends IGraphExecutionModel with Serializable {
  
    override def name(): String = "CreationRecommendationModel";
    override implicit val className = "org.ekstep.analytics.vidyavaani.job.CreationRecommendationModel";
    
    val getConceptsQuery = "MATCH path = (usr:User{type: 'author'})-[r:uses]-(cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) where cnc.contentCount > 0 AND r.lift > 1 return usr.IL_UNIQUE_ID, cnc.IL_UNIQUE_ID, cnc.gradeLevel, cnc.liveContentCount, r.confidence as conf , r.lift as lift ORDER BY lift DESC, conf DESC, cnc.liveContentCount ASC"
    val getLangsQuery = "MATCH (usr:User{type:'author'})-[r:createdIn]->(lan:domain{IL_FUNC_OBJECT_TYPE:'Language'}) where lan.contentCount > 0 AND r.lift > 1 return usr.IL_UNIQUE_ID, lan, r.confidence as conf , r.lift as lift ORDER BY lift DESC, conf DESC , lan.liveContentCount ASC"
    val getContentTypeQuery = "MATCH (usr:User{type:'author'})-[r:uses]->(cntt:ContentType) where cntt.contentCount > 0 AND r.lift > 1 return usr.IL_UNIQUE_ID, cntt.name, cntt.liveContentCount, r.confidence as conf , r.lift as lift ORDER BY lift DESC, conf DESC , cntt.liveContentCount ASC"
    
    override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
    	sc.parallelize(Seq(""));
    }

    override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        
        val author_reco_limit = config.getOrElse("author_reco_limit", 5).asInstanceOf[Int];
        val langReturnProps = config.getOrElse("lang_return_props", List("name","code","isoCode")).asInstanceOf[List[String]];
        
        val concepts = GraphQueryDispatcher.dispatch(getConceptsQuery).list().toArray();
        val langs = GraphQueryDispatcher.dispatch(getLangsQuery).list().toArray();
        val contentTypes = GraphQueryDispatcher.dispatch(getContentTypeQuery).list().toArray();
        
        val authorConcepts = concepts.map(x => x.asInstanceOf[org.neo4j.driver.v1.Record]).map{x => 
            val gradeLevel = if (x.get("cnc.gradeLevel").isNull()) List() 
            else x.get("cnc.gradeLevel").asList().toList.asInstanceOf[List[String]];
            (x.get("usr.IL_UNIQUE_ID").asString(), (x.get("cnc.IL_UNIQUE_ID").asString(), gradeLevel, x.get("cnc.liveContentCount").asLong(), x.get("conf").asDouble(), x.get("lift").asDouble()))}
        val authorConceptsRDD = sc.parallelize(authorConcepts).groupByKey().map(f => (f._1, f._2.toList.take(author_reco_limit).map(x => (x._1, x._2, x._5))))
        
        val authorLang = langs.map(x => x.asInstanceOf[org.neo4j.driver.v1.Record]).map{x => (x.get("usr.IL_UNIQUE_ID").asString(), (x.get("lan").asMap().toMap.asInstanceOf[Map[String, String]], x.get("conf").asDouble(), x.get("lift").asDouble()))}
        val authorLangRDD = sc.parallelize(authorLang).groupByKey().map(f => (f._1, f._2.toList.take(author_reco_limit).map(x => (x._1, x._3))))
        
        val authorContentType = contentTypes.map(x => x.asInstanceOf[org.neo4j.driver.v1.Record]).map{x => (x.get("usr.IL_UNIQUE_ID").asString(), (x.get("cntt.name").asString(), x.get("cntt.liveContentCount").asLong(), x.get("conf").asDouble(), x.get("lift").asDouble()))}
        val authorContentTypeRDD = sc.parallelize(authorContentType).groupByKey().map(f => (f._1, f._2.toList.take(author_reco_limit).map(x => (x._1, x._4))))
        
        val finalResult = authorConceptsRDD.join(authorLangRDD).join(authorContentTypeRDD).mapValues{ f =>
            val combinations = for(x <- f._1._1; y <- f._1._2; z <- f._2) yield (x, y, z)
            val combinedLift = combinations.map(x => (x._1._2, x._1._1, x._2._1, x._3._1, (x._1._3 + x._2._2 + x._3._2))).sortBy(f => f._4).reverse
            combinedLift.map{x => 
                val filteredLangMap = x._3.filter(f => langReturnProps.contains(f._1))
                Request(x._1, List(x._2), x._4, filteredLangMap, "Content")};
        }.map(f => RequestRecos(f._1, f._2))
        JobLogger.log("Total number of users with recommendations", Option(Map("count" -> finalResult.count)), INFO);
        finalResult.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.REQUEST_RECOS);
        ppQueries;
    }
}