package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase

//case class Content(d_content_id: String, d_tag: String, m_publish_date: DateTime, m_last_gen_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_devices: Long, m_avg_sess_device: Double)
case class User(name: String, last_name: String, age: Int, city: String)

object PopulateNeo4jJob {

    def main()(implicit sc: Option[SparkContext] = None) {

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, "Neo4j Update");
            try {
              _updateGraph();
            } catch {
              case t: Throwable => t.printStackTrace() // TODO: handle error
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            _updateGraph();
        }

    }

    private def _updateGraph()(implicit sc: SparkContext) {

        val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("neo4j", "shanti10"))
        val session = driver.session
        
       val query =  """MATCH (n) WHERE EXISTS(n.author) RETURN DISTINCT "node" as element, n.author AS author UNION ALL MATCH ()-[r]-() WHERE EXISTS(r.author) RETURN DISTINCT "relationship" AS element, r.author AS author"""
       //val query = "MATCH (a:domain) WHERE n.IL_FUNC_OBJECT_TYPE = 'Content' AND EXISTS(n.author) RETURN a"
      
        
//        val user = User("Amit", "Behera", 27, "Bangalore")
//        val script = s"CREATE (user:Users {name:'${user.name}',last_name:'${user.last_name}',age:${user.age},city:'${user.city}'})"
//        println(script)
        val result = session.run(query)
//        session.close()
//        val script2 = "MATCH (user:Users) RETURN user.name AS name, user.last_name AS last_name, user.age AS age, user.city AS city"
//        val result2 = session.run(script2)
//
        while (result.hasNext()) {
            val record = result.next()
            val auth = record.get("author").toString()
            session.run(s"CREATE (n:author {authorName: '${auth.replace("'", "")}'})")
            println(auth)
        }
        session.close()
        println("Done!!!")
    }
}