package org.ekstep.analytics.streaming

import org.neo4j.driver.v1.Session
import org.neo4j.driver.v1.StatementResult
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.neo4j.driver.v1.Driver 
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.commons.lang3.StringUtils

class GraphQuerySink(createSession: () => Driver) extends Serializable  {
	
	lazy val driver = createSession();

	// TODO: We should enhance this. For every query we are creating a new session. we should use a single session for all.
	// Try by creating Transaction. 
    def run(query: String) : StatementResult = {
    	val session = driver.session();
    	val result = session.run(query);
    	session.close();
    	result;
    }
    
//    def close(): Unit = session.close();
}

object GraphQuerySink {
	def apply(config: Map[String, AnyRef]): GraphQuerySink = {
        val f = () => {
        	val driver = getDriver(config)
            sys.addShutdownHook {
                //session.close()
            }
            driver;
        }
        new GraphQuerySink(f);
    }
	
	private def getDriver(config: Map[String, AnyRef]) : Driver = {
		val isEmbedded = AppConf.getConfig("graph.service.embedded.enable");
		if (StringUtils.isNotBlank(isEmbedded) && StringUtils.equalsIgnoreCase("true", isEmbedded)) {
			val dbConfig = org.neo4j.driver.v1.Config.build().withEncryptionLevel(org.neo4j.driver.v1.Config.EncryptionLevel.NONE).toConfig(); 
			GraphDatabase.driver(config.getOrElse("url", "bolt://localhost:7687").asInstanceOf[String], dbConfig)	
		} else {
			val authToken = AuthTokens.basic(config.getOrElse("user", "neo4j").asInstanceOf[String], config.getOrElse("password", "neo4j").asInstanceOf[String]);
			GraphDatabase.driver(config.getOrElse("url", "bolt://localhost:7687").asInstanceOf[String], authToken);	
		}
	}
}