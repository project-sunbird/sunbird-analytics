package org.ekstep.analytics.framework

/**
 * @author Santhosh
 */
class TestModels extends BaseSpec {

    "Models" should "create model objects for raw event" in {

        val gdata = new GData("org.ekstep.aser", "1.0", null);
        val eks = new Eks(null, Array("MC1", "MC2"), Array("MC3"), "Yes", "eks.q.1", "MCQ",
            "MEDIUM", 1, 1, Array("something"), Array("Nothing"), "123",
            156.00, 1, 0, "READING", "Can read story", "Can read story",
            "TOUCH", "DRAG", "stage1:next",
            "org.ekstep.aser")
        val edata = new EData(eks, null);
        val event = new Event("OE_TEST", "2015-09-23T09:32:11+05:30", "2015-09-23T09:32:11+05:30", "1.0", gdata, "569b2478-c8da-4e21-8887-a0dfc2b47d7a", "569b2478-c8da-4e21-8887-a0dfc2b47d7a", "569b2478-c8da-4e21-8887-a0dfc2b47d7a", edata)
        
        event.eid should be ("OE_TEST");
    }

}