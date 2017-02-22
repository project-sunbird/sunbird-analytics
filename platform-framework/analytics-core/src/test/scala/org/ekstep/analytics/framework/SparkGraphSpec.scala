package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.util.CommonUtil
import org.neo4j.driver.v1.Session

class SparkGraphSpec extends SparkSpec {

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