package org.ekstep.analytics.model

import org.neo4j.driver.v1.Session
import org.ekstep.analytics.framework.util.CommonUtil

class SparkGraphSpec(override val file: String =  "src/test/resources/sample_telemetry.log") extends SparkSpec {
	
	implicit var session: Session = null;
	
	override def beforeAll() {
        super.beforeAll()
        session = CommonUtil.getGraphDbSession();
    }

    override def afterAll() {
        CommonUtil.closeGraphDbSession();
        super.afterAll();
    }
  
}