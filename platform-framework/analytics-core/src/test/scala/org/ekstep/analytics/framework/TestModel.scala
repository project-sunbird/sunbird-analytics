package org.ekstep.analytics.framework

class TestModel extends BaseSpec {
    it should "pass the test case by creatinig objects of the classes in Model class" in {
        val cdata = new CData(null, None)
        val content = new Content(null, null, None, None)
        val context = new Context(null, None, null, null)
        val dimensions = new Dimensions(None, None, None, None, None, None)
        val dispatcher = new Dispatcher(null, null)
        val dtRange = new DtRange(10l, 20l)
        val eData = new EData(null)
        val eks = new Eks(null, null, null, null, null, null, null, 0, 0, null, null, null, 0d, 0, 0, null, null, null, null, null, null, null)
        val event = new Event(null, null, null, null, null, null, null, null, null)
        val fetcher = new Fetcher(null, None, None)
        val filter = new Filter(null, null, None)
        val gData = new GData(null, null)
        val item = new Item(null,null,null,null,null)
        val itemSet = new ItemSet(null,null,null,null,0)   
        val jobConfig = new JobConfig(null,null,null,null,null,null,null,null)
        val measuredEvent = new MeasuredEvent(null,0l,null,null,null,null,null,null,null) 
        val meeData = new MEEdata(null)
        val metaData = new Metadata(null)
        val microConcept = new MicroConcept(null,null)
        val params = new Params(None,None,None,None,None)
        val pData = new PData(null,null,null) 
        val query = new Query(None,None,None,None,None,None,None,None,None,None)
        val questionnaire = new Questionnaire(null,null,null,null,null)
        val request = new Request(metaData,0)
        
    }
}